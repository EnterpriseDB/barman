#!/usr/bin/env python
#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011-2016 2ndQuadrant Italia Srl <info@2ndquadrant.it>
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

Barman (Backup and Recovery Manager) is an open source administration
tool for disaster recovery of PostgreSQL servers written in Python.
It allows your organisation to perform remote backups of multiple servers
in business critical environments and help DBAs during the recovery
phase. Barman's most requested features include backup catalogues,
incremental backup, retention policies, remote backup and recovery,
archiving and compression of WAL files and backups.

Barman is written and maintained by PostgreSQL professionals 2ndQuadrant.
"""

import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.version_info < (2, 6):
    raise SystemExit('ERROR: Barman needs at least python 2.6 to work')

# Depend on pytest_runner only when the tests are actually invoked
needs_pytest = set(['pytest', 'test']).intersection(sys.argv)
pytest_runner = ['pytest_runner'] if needs_pytest else []

setup_requires = pytest_runner

install_requires = [
    'psycopg2 >= 2.4.2',
    'argh >= 0.21.2',
    'python-dateutil',
    'argcomplete',
]

if sys.version_info < (2, 7):
    install_requires += [
        'argparse',
    ]

barman = {}
with open('barman/version.py', 'r') as fversion:
    exec(fversion.read(), barman)

setup(
    name='barman',
    version=barman['__version__'],
    author='2ndQuadrant Italia Srl',
    author_email='info@2ndquadrant.it',
    url='http://www.pgbarman.org/',
    packages=['barman', ],
    scripts=['bin/barman', ],
    data_files=[
        ('share/man/man1', ['doc/barman.1']),
        ('share/man/man5', ['doc/barman.5']),
    ],
    license='GPL-3.0',
    description=__doc__.split("\n")[0],
    long_description="\n".join(__doc__.split("\n")[2:]),
    install_requires=install_requires,
    platforms=['Linux', 'Mac OS X'],
    classifiers=[
        'Environment :: Console',
        'Development Status :: 5 - Production/Stable',
        'Topic :: System :: Archiving :: Backup',
        'Topic :: Database',
        'Topic :: System :: Recovery Tools',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License v3 or later '
        '(GPLv3+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    setup_requires=setup_requires,
    tests_require=[
        'pytest',
        'mock',
        'pytest-catchlog>=1.2.1',
        'pytest-timeout',
    ],
)
