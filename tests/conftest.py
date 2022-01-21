# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
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

import logging
import psycopg2
import pytest
import mock


@pytest.fixture(scope="session", autouse=True)
def default_session_fixture(request):
    """
    Make sure that any real connection to Postgres results in an error

    :type request: _pytest.python.SubRequest
    :return:
    """
    logging.info("Patching barman.postgres.psycopg2.connect")
    connect_patch = mock.patch("barman.postgres.psycopg2.connect")
    connect_mock = connect_patch.__enter__()
    connect_mock.side_effect = psycopg2.DatabaseError

    def unpatch():
        connect_patch.__exit__([None])
        logging.info("Unpatching barman.postgres.psycopg2.connect")

    request.addfinalizer(unpatch)
