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
import connexion
import os
import logging
from logging.config import dictConfig

import barman
from barman import config, output
from server import encoder


LOG_FILENAME = '/var/log/barman/barman-api.log'


@arg(
    '--port',
    help='port to run the REST app on',
    default=7480
    )
@expects_obj  # futureproofing for possible future args
def serve(args):
    """
    Run the Barman API app.
    """

    # load barman configs/setup barman for the app
    cfg = config.Config('/etc/barman.conf')
    barman.__config__ = cfg
    cfg.load_configuration_files_directory()
    output.set_output_writer(output.AVAILABLE_WRITERS['json']())

    # setup flask logging
    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        }},
        'handlers': {'wsgi': {
            'class': 'logging.FileHandler',
            'filename': LOG_FILENAME,
            'formatter': 'default'
        }},
        'root': {
            'level': 'INFO',
            'handlers': ['wsgi']
        }
    })

    # setup and run the app
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
    logging.basicConfig(filename='/var/log/barman/barman-api.log',
                        level=logging.INFO)
    logger = logging.getLogger(__name__)

    p = ArghParser(epilog="Barman API by EnterpriseDB (www.enterprisedb.com)")
    p.add_commands(
        [
            serve,
            status
        ])
    try:
        p.dispatch()
    except KeyboardInterrupt:
        logger.error("Process interrupted by user (KeyboardInterrupt)")
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    main()