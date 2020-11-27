# --
# Copyright (c) 2008-2020 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

from __future__ import absolute_import

import time
from functools import partial
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse
import memcache

from nagare.services import plugin


KEY_PREFIX = 'nagare_%d_'


class Lock(object):
    def __init__(self, connection, lock_id, ttl, poll_time, max_wait_time, noreply=False):
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
        self.noreply = False

    def acquire(self):
        """Acquire the lock
        """
        t0 = time.time()

        status = False
        while not status and (time.time() < (t0 + self.max_wait_time)):
            status = self.connection.add(self.lock, 1, self.ttl, noreply=self.noreply)
            if type(status) is int:
                break

            time.sleep(self.poll_time)

        return status

    __enter__ = acquire

    def release(self, *args):
        """Release the lock
        """
        self.connection.delete(self.lock)

    __exit__ = release


class Memcache(plugin.Plugin):
    """Sessions manager for sessions kept in an external memcached server
    """
    LOAD_PRIORITY = 75
    CONFIG_SPEC = dict(
        plugin.Plugin.CONFIG_SPEC,
        uri='string(default=None)',
        socket='string(default=None)',
        host='string(default="127.0.0.1")',
        port='integer(default=11211)',
        weight='integer(default=1)',

        debug='boolean(default=False)',
        max_key_length='integer(default={})'.format(memcache.SERVER_MAX_KEY_LENGTH),
        max_value_length='integer(default=0)',
        dead_retry='integer(default={})'.format(memcache._DEAD_RETRY),
        check_keys='boolean(default=False)',

        __many__={
            'uri': 'string(default=None)',
            'socket': 'string(default=None)',
            'host': 'string(default="127.0.0.1")',
            'port': 'integer(default=11211)',
            'weight': 'integer(default=1)'
        }
    )

    def __init__(
        self,
        name, dist,
        uri=None, socket=None, host='127.0.0.1', port=11211, weight=1,
        debug=False,
        max_key_length=memcache.SERVER_MAX_KEY_LENGTH,
        max_value_length=memcache.SERVER_MAX_VALUE_LENGTH,
        dead_retry=memcache._DEAD_RETRY,
        check_keys=False,
        **hosts
    ):
        """Initialization

        In:
          - ``host`` -- address of the memcache server
          - ``port`` -- port of the memcache server
          - ``debug`` -- display the memcache requests / responses
        """
        super(Memcache, self).__init__(
            name, dist,
            uri=uri, socket=socket, host=host, port=port, weight=weight,
            debug=debug,
            max_key_length=max_key_length,
            max_value_length=max_value_length,
            dead_retry=dead_retry,
            check_keys=check_keys,
            **hosts
        )

        self.hosts = []
        hosts = list(hosts.values()) or [{'uri': uri, 'socket': socket, 'host': host, 'port': port, 'weight': weight}]
        for host in hosts:
            if host['host'] and host['port']:
                addr = '{}:{}'.format(host['host'], host['port'])

            if host['socket']:
                addr = 'unix:{}'.format(host['socket'])

            if host['uri']:
                uri = urlparse.urlparse(host['uri'])
                if uri.scheme == 'memcached':
                    addr = '{}:{}'.format(uri.hostname or '127.0.0.1', uri.port or 11211)

            self.hosts.append(addr)

        self.debug = debug
        self.max_key_length = max_key_length
        self.max_value_length = max_value_length
        self.dead_retry = dead_retry
        self.check_keys = check_keys

        self.memcache = None

    def handle_start(self, app):
        self.memcache = memcache.Client(
            self.hosts, self.debug, -1,
            server_max_key_length=self.max_key_length,
            dead_retry=self.dead_retry,
            check_keys=self.check_keys
        )

        for name, f in memcache.Client.__dict__.items():
            if not name.startswith(('_', 'Memcached')):
                setattr(self, name, partial(f, self.memcache))

        server_max_value_lengths = [
            int(settings.get('item_size_max', '0'))
            for host, settings in self.get_stats('settings')
        ]
        server_max_value_length = min(server_max_value_lengths) if server_max_value_lengths else 0

        if server_max_value_length and self.max_value_length and (server_max_value_length != self.max_value_length):
            msg = 'Memcache configuration value `max_value_length` differs from detected value {}'
            self.logger.warning(msg.format(server_max_value_length))

        server_max_value_length = server_max_value_length or self.max_value_length or memcache.SERVER_MAX_VALUE_LENGTH
        self.memcache.server_max_value_length = server_max_value_length
        msg = 'Memcache `max_value_length` parameter set to {}'
        self.logger.info(msg.format(self.memcache.server_max_value_length))

    def flush_all(self):
        """Delete all the contents in the memcached server
        """
        memcached = memcache.Client(self.hosts, debug=self.debug)
        memcached.flush_all()

    def get_lock(self, lock_id, lock_ttl, lock_poll_time, lock_max_wait_time, noreply=False):
        """Retrieve the lock of a session

        In:
          - ``lock_id`` -- lock identifier

        Return:
          - the lock
        """
        return Lock(self, lock_id, lock_ttl, lock_poll_time, lock_max_wait_time, noreply)
