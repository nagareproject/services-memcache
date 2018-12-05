# --
# Copyright (c) 2008-2018 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

from __future__ import absolute_import

import time
from functools import partial

import memcache
from nagare.services import plugin


KEY_PREFIX = 'nagare_%d_'


class Lock(object):
    def __init__(self, connection, lock_id, ttl, poll_time, max_wait_time):
        """Distributed lock in memcache

        In:
          - ``connection`` -- connection object to the memcache server
          - ``lock_id`` -- unique lock identifier
          - ``ttl`` -- session locks timeout, in seconds (0 = no timeout)
          - ``poll_time`` -- wait time between two lock acquisition tries, in seconds
          - ``max_wait_time`` -- maximum time to wait to acquire the lock, in seconds
        """
        self.connection = connection
        self.lock = (KEY_PREFIX + 'lock') % lock_id
        self.ttl = ttl
        self.poll_time = poll_time
        self.max_wait_time = max_wait_time

    def acquire(self):
        """Acquire the lock
        """
        t0 = time.time()
        while not self.connection.add(self.lock, 1, self.ttl) and (time.time() < (t0 + self.max_wait_time)):
            time.sleep(self.poll_time)

    def release(self):
        """Release the lock
        """
        self.connection.delete(self.lock)


class Memcache(plugin.Plugin):
    """Sessions manager for sessions kept in an external memcached server
    """
    LOAD_PRIORITY = 75
    CONFIG_SPEC = {
        'host': 'string(default="127.0.0.1")',
        'port': 'integer(default=11211)',
        'debug': 'boolean(default=False)'
    }

    def __init__(self, name, dist, host='127.0.0.1', port=11211, debug=False):
        """Initialization

        In:
          - ``host`` -- address of the memcache server
          - ``port`` -- port of the memcache server
          - ``debug`` -- display the memcache requests / responses
        """
        super(Memcache, self).__init__(name, dist)

        self.hosts = ['%s:%d' % (host, port)]
        self.debug = debug

        self.memcache = None

    def handle_start(self, app):
        self.memcache = memcache.Client(self.hosts, debug=self.debug)

        for name, f in memcache.Client.__dict__.items():
            if not name.startswith(('_', 'Memcached')):
                setattr(self, name, partial(f, self.memcache))

    def flush_all(self):
        """Delete all the contents in the memcached server
        """
        memcached = memcache.Client(self.hosts, debug=self.debug)
        memcached.flush_all()

    def get_lock(self, lock_id, lock_ttl, lock_poll_time, lock_max_wait_time):
        """Retrieve the lock of a session

        In:
          - ``lock_id`` -- lock identifier

        Return:
          - the lock
        """
        return Lock(self, lock_id, lock_ttl, lock_poll_time, lock_max_wait_time)