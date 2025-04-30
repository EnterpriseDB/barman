#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# barman - Backup and Recovery Manager for PostgreSQL
#
# © Copyright EnterpriseDB UK Limited 2011-2025
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

if sys.version_info < (3, 6):
    raise SystemExit("ERROR: Barman needs at least python 3.6 to work")

# Depend on pytest_runner only when the tests are actually invoked
needs_pytest = set(["pytest", "test"]).intersection(sys.argv)
pytest_runner = ["pytest_runner"] if needs_pytest else []

setup_requires = pytest_runner

install_requires = [
    "psycopg2 >= 2.4.2",
    "python-dateutil",
]

barman = {}
with open("barman/version.py", "r", encoding="utf-8") as fversion:
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
                "docs/_build/man/barman.1",
                "docs/_build/man/barman-archive-wal.1",
                "docs/_build/man/barman-backup.1",
                "docs/_build/man/barman-check.1",
                "docs/_build/man/barman-check-backup.1",
                "docs/_build/man/barman-cloud-backup.1",
                "docs/_build/man/barman-cloud-backup-delete.1",
                "docs/_build/man/barman-cloud-backup-keep.1",
                "docs/_build/man/barman-cloud-backup-list.1",
                "docs/_build/man/barman-cloud-backup-show.1",
                "docs/_build/man/barman-cloud-check-wal-archive.1",
                "docs/_build/man/barman-cloud-restore.1",
                "docs/_build/man/barman-cloud-wal-archive.1",
                "docs/_build/man/barman-cloud-wal-restore.1",
                "docs/_build/man/barman-config-switch.1",
                "docs/_build/man/barman-config-update.1",
                "docs/_build/man/barman-cron.1",
                "docs/_build/man/barman-delete.1",
                "docs/_build/man/barman-diagnose.1",
                "docs/_build/man/barman-generate-manifest.1",
                "docs/_build/man/barman-get-wal.1",
                "docs/_build/man/barman-keep.1",
                "docs/_build/man/barman-list_backups.1",
                "docs/_build/man/barman-list-files.1",
                "docs/_build/man/barman-list-processes.1",
                "docs/_build/man/barman-list-servers.1",
                "docs/_build/man/barman-lock-directory-cleanup.1",
                "docs/_build/man/barman-put-wal.1",
                "docs/_build/man/barman-rebuild-xlogdb.1",
                "docs/_build/man/barman-receive-wal.1",
                "docs/_build/man/barman-restore.1",
                "docs/_build/man/barman-replication-status.1",
                "docs/_build/man/barman-show-backup.1",
                "docs/_build/man/barman-show-servers.1",
                "docs/_build/man/barman-status.1",
                "docs/_build/man/barman-switch-wal.1",
                "docs/_build/man/barman-switch-xlog.1",
                "docs/_build/man/barman-sync-backup.1",
                "docs/_build/man/barman-sync-info.1",
                "docs/_build/man/barman-sync-wals.1",
                "docs/_build/man/barman-terminate-process.1",
                "docs/_build/man/barman-verify.1",
                "docs/_build/man/barman-verify-backup.1",
                "docs/_build/man/barman-wal-restore.1",
                "docs/_build/man/barman-wal-archive.1",
            ],
        ),
        ("share/man/man5", ["docs/_build/man/barman.5"]),
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
            "barman-cloud-backup-show=barman.clients.cloud_backup_show:main",
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
        "argcomplete": ["argcomplete"],
        "aws-snapshots": ["boto3"],
        "azure": ["azure-identity", "azure-storage-blob"],
        "azure-snapshots": ["azure-identity", "azure-mgmt-compute"],
        "cloud": ["boto3"],
        "google": [
            "google-cloud-storage",
        ],
        "google-snapshots": [
            "grpcio",
            "google-cloud-compute",  # requires minimum python3.7
        ],
        "snappy": [
            'python-snappy==0.6.1; python_version<"3.7"',
            'python-snappy; python_version>="3.7"',
            'cramjam >= 2.7.0; python_version>="3.7"',
        ],
        "zstandard": ["zstandard"],
        "lz4": ["lz4"],
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
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    setup_requires=setup_requires,
)
