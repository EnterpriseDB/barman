#!/usr/bin/env python
#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011-2012  2ndQuadrant Italia (Devise.IT S.r.l.) <info@2ndquadrant.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Backup and Recovery Manager for PostgreSQL

Barman (backup and recovery manager) is an administration
tool for disaster recovery of PostgreSQL servers written in Python.
It allows to perform remote backups of multiple servers
in business critical environments and help DBAs during the recovery phase.
Barman's most wanted features include backup catalogs, retention policies,
remote recovery, archiving and compression of WAL files and backups.
Barman is written and maintained by PostgreSQL professionals 2ndQuadrant.
"""

import sys
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.version_info < (2 , 6):
    raise SystemExit('ERROR: Barman need at least python 2.6 to work')

REQUIRES = ['psycopg2', 'argh', 'python-dateutil < 2.0' ]

if sys.version_info < (2 , 7):
    REQUIRES.append('argparse')

barman = {}
execfile('barman/version.py', barman)

setup(
    name='barman',
    version=barman['__version__'],
    author='2ndQuadrant Italia (Devise.IT S.r.l.)',
    author_email='info@2ndquadrant.it',
    url='http://www.pgbarman.org/',
    packages=['barman', 'barman.test', ],
    scripts=['bin/barman', ],
    data_files=[
        ('share/man/man1', ['doc/barman.1']),
        ('share/man/man5', ['doc/barman.5']),
        ],
    license='GPL-3.0',
    description=__doc__.split("\n")[0],
    long_description="\n".join(__doc__.split("\n")[2:]),
    install_requires=REQUIRES,
    platforms=['Linux', 'Mac OS X'],
    classifiers=[
        'Topic :: System :: Archiving :: Backup',
        'Topic :: Database',
        'Topic :: System :: Recovery Tools',
        ],
)
