# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2026
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

"""
This module contains the current Barman version.
"""

# importlib.metadata reads the version from the dist-info metadata written
# to site-packages at install time. The version is defined once in
# pyproject.toml and embedded in that metadata during the build, so there is
# no need to maintain a hardcoded string here. The fallback to "unknown"
# applies when barman is run directly from source without being installed
# (e.g. `python -m barman` in a bare clone). In normal usage —
# pip install, uv sync, tox — the package is always installed first.
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("barman")
except PackageNotFoundError:
    __version__ = "unknown"
