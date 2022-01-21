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

from barman.postgres_plumbing import function_name_map


class TestFunctionNameMap(object):
    def test_null_server_version(self):
        result = function_name_map(None)
        assert result["pg_switch_wal"] == "pg_switch_wal"

    def test_postgresql_10(self):
        result = function_name_map(100100)
        assert result["pg_switch_wal"] == "pg_switch_wal"

    def test_postgresql_9(self):
        result = function_name_map(90100)
        assert result["pg_switch_wal"] == "pg_switch_xlog"
