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

import os
import connexion
from server import encoder

def main():
    app = connexion.App(__name__, specification_dir='./spec/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('barman_api.yaml',
                arguments={'title': 'Barman API'},
                pythonic_params=True)

    app.run(host='127.0.0.1', port=7480)

if __name__ == '__main__':
    main()