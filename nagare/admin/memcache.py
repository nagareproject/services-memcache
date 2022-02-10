# --
# Copyright (c) 2008-2022 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

from nagare.admin import command


class Commands(command.Commands):
    DESC = 'Memcache subcommands'


class Stats(command.Command):
    DESC = 'displays general statistics'
    WITH_STARTED_SERVICES = True

    def run(self, memcache_service, name=None):
        stats = memcache_service.get_stats(name)

        nb_servers = len(stats)
        print('{} server{} found\n'.format(nb_servers, '' if nb_servers == 1 else 's'))

        for server, stats in sorted(stats):
            print('{}:'.format(server))

            if not stats:
                print('  <empty>')
            else:
                for stat, value in sorted(stats.items()):
                    print('  - {}: {}'.format(stat, value))

        return nb_servers == 0


class Report(Stats):
    DESC = 'displays cache status'

    def set_arguments(self, parser):
        parser.add_argument(
            'name',
            choices=sorted(('settings', 'items', 'sizes', 'sizes_enable', 'sizes_disable', 'slabs', 'conns')),
            help='name of report to display'
        )

        super(Report, self).set_arguments(parser)


class Flush(command.Command):
    DESC = 'delete all the keys in cache'
    WITH_STARTED_SERVICES = True

    def run(self, memcache_service):
        memcache_service.flush_all()
