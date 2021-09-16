# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

from argh import ArghParser, arg, expects_obj
import os
import connexion

from server import encoder


@arg(
    '--port',
    help='port to run the REST app on',
    default=7480
    )
@expects_obj  # futureproofing for possible future args
def serve(args):
    app = connexion.App(__name__, specification_dir='./spec/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('barman_api.yaml',
                arguments={'title': 'Barman API'},
                pythonic_params=True)

    # bc currently only the PEM agent will be connecting, only run on localhost
    app.run(host='127.0.0.1', port=args.port)


def status(args):
    pass


def main():
    """
    Main method of the Barman API app
    """
    p = ArghParser(epilog="Barman API by EnterpriseDB (www.enterprisedb.com)")
    p.add_commands(
        [
            serve,
            status
        ])
    try: 
        p.dispatch()
    except KeyboardInterrupt:
        msg = "Process interrupted by user (KeyboardInterrupt)"
        print(msg)  # TODO logging
    except Exception as e:
        pass  # TODO logging


if __name__ == '__main__':
    main()