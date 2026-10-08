# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""List-then-watch over the Kubernetes API, resumed by resourceVersion.

Every write the API server stores gets a new resourceVersion, and a watch
started at resourceVersion R delivers every stored write after R to the objects
it selects, in order, or fails. It does not skip one: a watch cache that no
longer holds R answers 410 Gone, and a watcher that falls behind is
disconnected. So windows chained by `Window.resource_version`, which includes
bookmarks, see every stored version of a selected object, and two consecutive
`Change`s for one object are two consecutive stored versions of it. A write
that changes nothing is not stored, so it is not a version.

A relist reads only the current version. Versions stored between the last
delivered item and the listing are never delivered, which `Relist` marks.

resourceVersions are opaque, so they are only ever compared for equality.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import urllib3
from kubernetes import watch  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore

# Consecutive failed windows after which the next one relists instead of
# resuming. A resume point the API server cannot serve is usually answered
# with 410, which relists at once, but one ahead of the server (after a state
# reset) is answered with a timeout instead.
RELIST_AFTER_FAILURES = 3
# Backoff after a failed window, doubled per consecutive failure.
BACKOFF_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 8.0

ERRORS: tuple[type[BaseException], ...] = (
    ApiException,
    urllib3.exceptions.HTTPError,
    OSError,
    TimeoutError,
    ValueError,
    KeyError,
)


def resource_version(obj: dict[str, Any]) -> str | None:
    return (obj.get("metadata") or {}).get("resourceVersion")


@dataclass(frozen=True)
class Relist:
    """Every selected object at one consistent read.

    Not adjacent to anything delivered before it.
    """

    items: list[dict[str, Any]]
    resource_version: str
    """The listing's collection resourceVersion, where the watch resumes."""


@dataclass(frozen=True)
class Change:
    """One stored write: `ADDED`, `MODIFIED` or `DELETED`, and the object as written."""

    type: str
    obj: dict[str, Any]

    @property
    def resource_version(self) -> str | None:
        return resource_version(self.obj)


Item = Relist | Change


@dataclass(frozen=True)
class Window:
    items: list[Item]
    """In the order the API server stored them."""
    resource_version: str | None
    """Where the next window resumes, or None to relist."""
    error: str | None = None
    """Why the window ended early. Its items are still valid."""


class ResourceWatch:
    """One list function, watched in windows of `window_seconds`.

    `list_fn` is a generated client list method. `request_timeout` must be an
    int: the generated client silently drops a float timeout. Every window
    opens a new watch request, so a dropped connection or a server-side
    timeout loses nothing as long as the caller resumes from
    `Window.resource_version`.
    """

    def __init__(
        self,
        list_fn: Callable[..., Any],
        *args: Any,
        window_seconds: int,
        request_timeout: int,
        sleep: Callable[[float], None] = time.sleep,
        **kwargs: Any,
    ) -> None:
        self.list_fn = list_fn
        self.args = args
        self.kwargs = kwargs
        self.window_seconds = window_seconds
        self.request_timeout = request_timeout
        self.sleep = sleep
        self.failures = 0

    def window(self, resume_from: str | None) -> Window:
        """Changes after `resume_from`, preceded by a `Relist` if it is None."""
        items: list[Item] = []
        rv = resume_from if self.failures < RELIST_AFTER_FAILURES else None
        try:
            if rv is None:
                response = self.list_fn(
                    *self.args,
                    **self.kwargs,
                    _preload_content=False,
                    _request_timeout=self.request_timeout,
                )
                body = json.loads(response.data)
                rv = body["metadata"]["resourceVersion"]
                assert rv is not None
                items.append(Relist(body.get("items") or [], rv))
            # `timeout_seconds` also stops `Watch.stream` from retrying on its
            # own, which would resume a 410 from the same resourceVersion.
            stream = watch.Watch(return_type="object").stream(
                self.list_fn,
                *self.args,
                **self.kwargs,
                resource_version=rv,
                allow_watch_bookmarks=True,
                timeout_seconds=self.window_seconds,
                _request_timeout=(
                    self.request_timeout,
                    self.window_seconds + self.request_timeout,
                ),
            )
            for event in stream:
                obj = event["raw_object"]
                rv = resource_version(obj) or rv
                if event["type"] != "BOOKMARK":
                    items.append(Change(event["type"], obj))
        except ApiException as e:
            if e.status == 410:
                return Window(items, None, f"410 {e.reason}")
            self._backoff()
            return Window(items, rv, f"{e.status} {e.reason}")
        except ERRORS as e:
            self._backoff()
            return Window(items, rv, f"{type(e).__name__}: {e}")
        self.failures = 0
        return Window(items, rv)

    def _backoff(self) -> None:
        self.failures += 1
        self.sleep(min(BACKOFF_SECONDS * 2 ** (self.failures - 1), BACKOFF_MAX_SECONDS))


@dataclass(frozen=True)
class Step:
    current: dict[str, Any] | None
    """The new version, or None if the object is absent or was deleted."""
    predecessor: dict[str, Any] | None
    """The version stored immediately before `current`, or None if unknown."""


@dataclass(frozen=True)
class ObjectChain:
    """How far a watch on one named object has been processed.

    `last` is the newest processed version of the object, or None if it is
    absent or unknown. A chain is a value, so a caller can compute the next
    one, persist it, and only then adopt it.
    """

    resource_version: str | None = None
    last: dict[str, Any] | None = None

    def key(self) -> tuple[str | None, str | None]:
        return (
            self.resource_version,
            resource_version(self.last) if self.last is not None else None,
        )

    def advance(self, item: Item) -> tuple[ObjectChain, Step | None]:
        """The chain after `item`, and the step it makes.

        The step is None if `item` holds no new version: a relist that finds
        the object at the version already processed. Equal resourceVersions
        mean no write in between, so such a relist keeps the chain adjacent.
        """
        if isinstance(item, Relist):
            current = item.items[0] if item.items else None
            unchanged = (
                current is None
                if self.last is None
                else current is not None
                and resource_version(current) == resource_version(self.last)
            )
            if unchanged:
                return ObjectChain(item.resource_version, self.last), None
            predecessor = None
        else:
            current = None if item.type == "DELETED" else item.obj
            predecessor = self.last
        rv = item.resource_version or self.resource_version
        return ObjectChain(rv, current), Step(current, predecessor)

    def resume_at(self, rv: str | None) -> ObjectChain:
        return ObjectChain(rv, self.last)

    def to_json(self) -> dict[str, Any]:
        return {"resource_version": self.resource_version, "last": self.last}

    @staticmethod
    def from_json(value: dict[str, Any] | None) -> ObjectChain:
        if not value:
            return ObjectChain()
        return ObjectChain(value.get("resource_version"), value.get("last"))
