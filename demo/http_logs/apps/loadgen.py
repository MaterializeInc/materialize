#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import requests
import time
import random
import bisect
from enum import Enum, auto
from collections import namedtuple
import string
import numpy as np
import time
import sys

class State(Enum):
    GATEWAY = 0
    SEARCH = auto()
    DETAIL = auto()
    QUIT = auto()
    DO_NOTHING = auto()


def default_sampler():
    return np.random.zipf(1.1) - 1


def gen_items(n):
    names = set()
    alnum = string.ascii_letters + string.digits

    def gen_name():
        old_len = len(names)
        x = ""
        while len(names) == old_len:
            x = ''.join(random.choices(alnum, k=8))
            names.add(x)
        assert x
        return x

    return [gen_name() for _ in range(n)]


def get_index(items, sampler):
    z = len(items)
    while z >= len(items):
        z = sampler()
    return z


def get_item(items, sampler=default_sampler):
    return items[get_index(items, sampler)]


def next_state(behavior, cur_state):
    cb = behavior[cur_state.value]
    return np.random.choice(State, p=cb / np.sum(cb))


DEFAULT_BEHAVIOR = [
    [1, 8, 2, 1, 1],  # gateway
    [1, 3, 3, 1, 1],  # search
    [1, 5, 2, 1, 1],  # detail
    [10, 2, 2, 0, 0],  # quit (used for new user)
]

DEFAULT_URL = 'http://server:5000'

DEFAULT_ITEMS = gen_items(1000000)

DEFAULT_NEW_USERS_PER_TICK = 100

DEFAULT_TICK_SLEEP_SECONDS = 1

with open('./words.txt') as f:
    WORDS = f.read().splitlines()

def path_for_state(state):
    if state == State.GATEWAY:
        return "/"
    if state == State.SEARCH:
        return "/search/?kw={}+{}+{}".format(random.choice(WORDS), random.choice(WORDS), random.choice(WORDS))
    if state == State.DETAIL:
        return "/detail/{}".format(get_item(DEFAULT_ITEMS))
    assert False  # This should not be called for other states


class User:
    def __init__(self, behavior=DEFAULT_BEHAVIOR):
        self.behavior = behavior
        self.state = next_state(self.behavior, State.QUIT)
        self.ip = '{}.{}.{}.{}'.format(
            random.randint(0, 127),
            random.randint(0, 127),
            random.randint(0, 127),
            random.randint(0, 127)
        )

    def take_action(self, base_url=DEFAULT_URL):
        assert self.state != State.QUIT
        if self.state == State.DO_NOTHING:
            self.state = self.old_state
            assert self.state != State.DO_NOTHING
        else:
            self.old_state = self.state
            url = '{}{}'.format(base_url, path_for_state(self.state))
            # TODO - throughput can be higher if this is made async
            requests.get(url, headers={'X-Forwarded-For': self.ip})
            self.state = next_state(self.behavior, self.state)


class Simulation:
    def __init__(self):
        self.users = []

    def tick(self):
        time_begin = time.time()
        self.users.extend((User() for _ in range(np.random.poisson(DEFAULT_NEW_USERS_PER_TICK))))
        old_len_users = len(self.users)
        random.shuffle(self.users)
        for u in self.users:
            u.take_action()
        self.users = [u for u in self.users if u.state != State.QUIT]
        elapsed = time.time() - time_begin
        print("{} users acted in {}s. {} quit.".format(old_len_users, elapsed, old_len_users - len(self.users)))
        sys.stdout.flush()
        if elapsed < DEFAULT_TICK_SLEEP_SECONDS:
            time.sleep(DEFAULT_TICK_SLEEP_SECONDS - elapsed)


if __name__ == '__main__':
    s = Simulation()
    while True:
        s.tick()
