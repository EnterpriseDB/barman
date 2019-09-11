#!/usr/bin/env python
#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011-2018 2ndQuadrant Limited <info@2ndquadrant.com>
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

Barman (Backup and Recovery Manager) is an open-source administration
tool for disaster recovery of PostgreSQL servers written in Python.
It allows your organisation to perform remote backups of multiple
servers in business critical environments to reduce risk and help DBAs
during the recovery phase.

Barman is distributed under GNU GPL 3 and maintained by 2ndQuadrant.
"""

import sys

from setuptools import find_packages, setup

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
]

if sys.version_info < (2, 7):
    install_requires += [
        'argparse',
    ]
    # If we are going to execute tests, we need to enforce wheel
    # version before installing mock, or it will fail
    if needs_pytest:
        setup_requires += [
            'wheel<0.30.0',  # wheel has dropped 2.6 support in 0.30.0
        ]

barman = {}
with open('barman/version.py', 'r') as fversion:
    exec(fversion.read(), barman)

setup(
    name='barman',
    version=barman['__version__'],
    author='2ndQuadrant Limited',
    author_email='info@2ndquadrant.com',
    url='http://www.pgbarman.org/',
    packages=find_packages(exclude=["tests"]),
    data_files=[
        ('share/man/man1', ['doc/barman.1',
                            'doc/barman-wal-archive.1',
                            'doc/barman-wal-restore.1']),
        ('share/man/man5', ['doc/barman.5']),
    ],
    entry_points={
        'console_scripts': [
            'barman=barman.cli:main',
            'barman-wal-archive=barman.clients.walarchive:main',
            'barman-wal-restore=barman.clients.walrestore:main',
        ],
    },
    license='GPL-3.0',
    description=__doc__.split("\n")[0],
    long_description="\n".join(__doc__.split("\n")[2:]),
    install_requires=install_requires,
    extras_require={
        'completion': ['argcomplete'],
    },
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
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    setup_requires=setup_requires,
    tests_require=[
        'mock',
        'pytest-timeout',
        'pytest',
    ],
)
