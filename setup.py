#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# barman - Backup and Recovery Manager for PostgreSQL
#
# © Copyright EnterpriseDB UK Limited 2011-2022
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

Barman is distributed under GNU GPL 3 and maintained by EnterpriseDB.
"""

import sys

from setuptools import find_packages, setup

if sys.version_info < (2, 7):
    raise SystemExit("ERROR: Barman needs at least python 2.7 to work")

# Depend on pytest_runner only when the tests are actually invoked
needs_pytest = set(["pytest", "test"]).intersection(sys.argv)
pytest_runner = ["pytest_runner"] if needs_pytest else []

setup_requires = pytest_runner

install_requires = [
    "psycopg2 >= 2.4.2",
    "python-dateutil",
    "argcomplete",
]

barman = {}
with open("barman/version.py", "r") as fversion:
    exec(fversion.read(), barman)

setup(
    name="barman",
    version=barman["__version__"],
    author="EnterpriseDB",
    author_email="barman@enterprisedb.com",
    url="https://www.pgbarman.org/",
    packages=find_packages(exclude=["tests"]),
    data_files=[
        (
            "share/man/man1",
            [
                "doc/barman.1",
                "doc/barman-cloud-backup.1",
                "doc/barman-cloud-backup-keep.1",
                "doc/barman-cloud-backup-list.1",
                "doc/barman-cloud-backup-delete.1",
                "doc/barman-cloud-check-wal-archive.1",
                "doc/barman-cloud-restore.1",
                "doc/barman-cloud-wal-archive.1",
                "doc/barman-cloud-wal-restore.1",
                "doc/barman-wal-archive.1",
                "doc/barman-wal-restore.1",
            ],
        ),
        ("share/man/man5", ["doc/barman.5"]),
    ],
    entry_points={
        "console_scripts": [
            "barman=barman.cli:main",
            "barman-cloud-backup=barman.clients.cloud_backup:main",
            "barman-cloud-wal-archive=barman.clients.cloud_walarchive:main",
            "barman-cloud-restore=barman.clients.cloud_restore:main",
            "barman-cloud-wal-restore=barman.clients.cloud_walrestore:main",
            "barman-cloud-backup-delete=barman.clients.cloud_backup_delete:main",
            "barman-cloud-backup-keep=barman.clients.cloud_backup_keep:main",
            "barman-cloud-backup-list=barman.clients.cloud_backup_list:main",
            "barman-cloud-check-wal-archive=barman.clients.cloud_check_wal_archive:main",
            "barman-wal-archive=barman.clients.walarchive:main",
            "barman-wal-restore=barman.clients.walrestore:main",
        ],
    },
    license="GPL-3.0",
    description=__doc__.split("\n")[0],
    long_description="\n".join(__doc__.split("\n")[2:]),
    install_requires=install_requires,
    extras_require={
        "cloud": ["boto3"],
        "azure": ["azure-identity", "azure-storage-blob"],
        "snappy": [
            "python-snappy == 0.6.0"
        ],  # version is limited py python2.7 see issue #529
        "google": [
            "google-cloud-storage",
        ],
    },
    platforms=["Linux", "Mac OS X"],
    classifiers=[
        "Environment :: Console",
        "Development Status :: 5 - Production/Stable",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: Database",
        "Topic :: System :: Recovery Tools",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    setup_requires=setup_requires,
)
