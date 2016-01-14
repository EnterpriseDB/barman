#!/bin/sh

# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
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

set -e

BASE="$(dirname $(cd $(dirname "$0"); pwd))"
cd "$BASE"

VERSION="$(python -c 'd={}; exec(open("barman/version.py").read(), d); print(d["__version__"])')"
scripts/gitlog-to-changelog > ChangeLog
git add ChangeLog
git commit -m "Update the ChangeLog file"
scripts/gitlog-to-changelog > ChangeLog
git add ChangeLog
git commit -m "Update the ChangeLog file" --amend
./setup.py sdist
if ! git tag -s -m "Release ${VERSION}" release/${VERSION}
then
  echo "Cannot tag the release as the private key is missing"
fi
