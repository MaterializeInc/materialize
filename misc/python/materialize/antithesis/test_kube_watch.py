# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import urllib3
from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis.kube_watch import (
    Change,
    ObjectChain,
    Relist,
    ResourceWatch,
)


def obj(rv: str, reason: str = "Applied") -> dict[str, Any]:
    return {"metadata": {"name": "mz", "resourceVersion": rv}, "status": reason}


class Response:
    def __init__(self, data: bytes = b"", lines: list[Any] | None = None) -> None:
        self.data = data
        self.lines = lines or []

    def stream(self, amt: Any = None, decode_content: Any = None) -> Iterator[bytes]:
        for line in self.lines:
            if isinstance(line, BaseException):
                raise line
            yield (json.dumps(line) + "\n").encode()

    def close(self) -> None:
        pass

    def release_conn(self) -> None:
        pass


class FakeApi:
    """A list function that answers lists and watches from canned responses."""

    def __init__(self, listing: dict[str, Any] | None = None) -> None:
        self.listing = listing
        self.watches: list[Any] = []
        self.calls: list[dict[str, Any]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Response:
        self.calls.append(kwargs)
        if kwargs.get("watch"):
            answer = self.watches.pop(0)
            if isinstance(answer, BaseException):
                raise answer
            return Response(lines=answer)
        assert self.listing is not None, "unexpected list"
        return Response(data=json.dumps(self.listing).encode())


def event(kind: str, o: dict[str, Any]) -> dict[str, Any]:
    return {"type": kind, "object": o}


def make(api: FakeApi, sleeps: list[float]) -> ResourceWatch:
    return ResourceWatch(api, window_seconds=1, request_timeout=10, sleep=sleeps.append)


def test_relist_then_changes_in_order() -> None:
    api = FakeApi({"metadata": {"resourceVersion": "10"}, "items": [obj("9")]})
    api.watches.append(
        [
            event("MODIFIED", obj("11")),
            event("BOOKMARK", {"metadata": {"resourceVersion": "12"}}),
        ]
    )
    window = make(api, []).window(None)
    assert window.items == [Relist([obj("9")], "10"), Change("MODIFIED", obj("11"))]
    assert window.resource_version == "12", "a bookmark advances the resume point"
    assert window.error is None
    assert api.calls[1]["resource_version"] == "10"


def test_resume_does_not_relist() -> None:
    api = FakeApi()
    api.watches.append([event("MODIFIED", obj("21"))])
    window = make(api, []).window("20")
    assert window.items == [Change("MODIFIED", obj("21"))]
    assert [c["resource_version"] for c in api.calls] == ["20"]


def test_expired_event_relists_next_window_without_backoff() -> None:
    api = FakeApi()
    api.watches.append(
        [
            event("MODIFIED", obj("21")),
            {
                "type": "ERROR",
                "object": {"code": 410, "reason": "Expired", "message": "too old"},
            },
        ]
    )
    sleeps: list[float] = []
    window = make(api, sleeps).window("20")
    assert window.items == [Change("MODIFIED", obj("21"))]
    assert window.resource_version is None
    assert sleeps == []


def test_expired_request_relists_next_window() -> None:
    api = FakeApi()
    api.watches.append(ApiException(status=410, reason="Gone"))
    window = make(api, []).window("20")
    assert window.items == [] and window.resource_version is None


def test_dropped_connection_resumes_after_last_delivered_change() -> None:
    api = FakeApi()
    api.watches.append(
        [event("MODIFIED", obj("21")), urllib3.exceptions.ProtocolError("reset")]
    )
    sleeps: list[float] = []
    window = make(api, sleeps).window("20")
    assert window.items == [Change("MODIFIED", obj("21"))]
    assert window.resource_version == "21"
    assert window.error is not None
    assert sleeps == [0.5]


def test_repeated_failures_relist() -> None:
    api = FakeApi({"metadata": {"resourceVersion": "30"}, "items": [obj("29")]})
    timeout = ApiException(status=504, reason="Timeout")
    api.watches.extend([timeout, timeout, timeout, []])
    sleeps: list[float] = []
    watch = make(api, sleeps)
    for _ in range(3):
        assert watch.window("99").resource_version == "99"
    window = watch.window("99")
    assert window.items == [Relist([obj("29")], "30")]
    assert window.resource_version == "30"
    assert sleeps == [0.5, 1.0, 2.0]
    assert watch.failures == 0


def test_until_ends_the_window_after_the_matching_change() -> None:
    api = FakeApi()
    api.watches.append(
        [
            event("MODIFIED", obj("21", "Applying")),
            event("MODIFIED", obj("22", "Promoting")),
            event("MODIFIED", obj("23", "Applied")),
        ]
    )
    window = make(api, []).window(
        "20",
        until=lambda item: isinstance(item, Change)
        and item.obj["status"] == "Promoting",
    )
    assert window.items == [
        Change("MODIFIED", obj("21", "Applying")),
        Change("MODIFIED", obj("22", "Promoting")),
    ]
    assert window.resource_version == "22", "resumes after the matching change"
    assert window.error is None


def test_until_can_end_the_window_at_the_relist() -> None:
    api = FakeApi({"metadata": {"resourceVersion": "10"}, "items": [obj("9")]})
    window = make(api, []).window(None, until=lambda item: isinstance(item, Relist))
    assert window.items == [Relist([obj("9")], "10")]
    assert window.resource_version == "10"
    assert len(api.calls) == 1, "no watch opened"


def test_changes_are_adjacent() -> None:
    chain = ObjectChain()
    chain, step = chain.advance(Relist([obj("9", "Applying")], "10"))
    assert step is not None and step.current == obj("9", "Applying")
    assert step.predecessor is None, "a relist is never adjacent"
    chain, step = chain.advance(Change("MODIFIED", obj("11", "Promoting")))
    assert step is not None and step.predecessor == obj("9", "Applying")
    chain, step = chain.advance(Change("MODIFIED", obj("12", "Applied")))
    assert step is not None and step.predecessor == obj("11", "Promoting")
    assert chain.resource_version == "12"


def test_relist_at_an_unchanged_version_keeps_adjacency() -> None:
    chain = ObjectChain("12", obj("11", "Promoting"))
    chain, step = chain.advance(Relist([obj("11", "Promoting")], "40"))
    assert step is None, "no new version"
    assert chain == ObjectChain("40", obj("11", "Promoting"))
    chain, step = chain.advance(Change("MODIFIED", obj("41", "Applied")))
    assert step is not None and step.predecessor == obj("11", "Promoting")


def test_relist_at_a_changed_version_breaks_adjacency() -> None:
    chain = ObjectChain("12", obj("11", "Promoting"))
    chain, step = chain.advance(Relist([obj("35", "Applying")], "40"))
    assert step is not None and step.current == obj("35", "Applying")
    assert step.predecessor is None
    chain, step = chain.advance(Change("MODIFIED", obj("41", "Promoting")))
    assert step is not None and step.predecessor == obj("35", "Applying")


def test_deletion_breaks_adjacency() -> None:
    chain = ObjectChain("12", obj("11"))
    chain, step = chain.advance(Change("DELETED", obj("13")))
    assert step is not None and step.current is None
    assert step.predecessor == obj("11")
    chain, step = chain.advance(Relist([], "14"))
    assert step is None, "still absent"
    chain, step = chain.advance(Change("ADDED", obj("15")))
    assert step is not None and step.predecessor is None


def test_resume_at_keeps_last_version() -> None:
    chain = ObjectChain("12", obj("11")).resume_at(None)
    assert chain == ObjectChain(None, obj("11"))
    assert ObjectChain.from_json(json.loads(json.dumps(chain.to_json()))) == chain
    assert ObjectChain.from_json(None) == ObjectChain()
