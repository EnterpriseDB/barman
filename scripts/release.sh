#!/bin/sh
# Copyright (C) 2011  Devise.IT S.r.l. <info@2ndquadrant.it>
set -e

BASE="$(dirname $(cd $(dirname "$0"); pwd))"
cd "$BASE"

VERSION="$(python -c 'd={}; execfile("barman/version.py", d); print d["__version__"]')"
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
