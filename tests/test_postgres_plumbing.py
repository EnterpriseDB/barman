# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2026
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

import pytest
from mock import Mock

from barman.postgres_plumbing import (
    EXCLUDE_LIST,
    function_name_map,
    get_tablespace_filters,
)


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


class TestGetTablespaceFilters(object):
    @pytest.mark.parametrize("server_major_version", ["9.6", "12", "17"])
    def test_postgres(self, server_major_version):
        """Test that only the version directory is included for Postgres."""
        # GIVEN a backup of a standard Postgres server
        backup_info = Mock(is_whpg=False)

        # WHEN the tablespace filters are built
        include, exclude = get_tablespace_filters(backup_info, server_major_version)

        # THEN only the anchored PG_<major_version>_* directory is included
        assert include == ["/PG_%s_*" % server_major_version]
        # AND everything else at the tablespace root is excluded
        assert exclude == ["/*"] + EXCLUDE_LIST

    @pytest.mark.parametrize(
        ("dbid", "whpg_major_version"),
        [(1, 6), (2, 7), (15, 7)],
    )
    def test_whpg(self, dbid, whpg_major_version):
        """Test that only the dbid version directory is included for WHPG."""
        # GIVEN a backup of a WarehousePG instance
        backup_info = Mock(
            is_whpg=True, whpg_dbid=dbid, whpg_major_version=whpg_major_version
        )

        # WHEN the tablespace filters are built
        # (the Postgres major version must be ignored for WarehousePG)
        include, exclude = get_tablespace_filters(backup_info, "12")

        # THEN only the dbid directory and its GPDB_<major_version>_* directory
        # are included, both anchored at the tablespace root
        assert include == [
            "/%s" % dbid,
            "/%s/GPDB_%s_*" % (dbid, whpg_major_version),
        ]
        # AND everything else at the root or directly under the dbid directory
        # is excluded, with the dbid exclude anchored so it cannot match the
        # directory of a database whose OID equals the dbid
        assert exclude == ["/*", "/%s/*" % dbid] + EXCLUDE_LIST
