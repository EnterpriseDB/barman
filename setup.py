#!/usr/bin/env python
#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011-2014 2ndQuadrant Italia (Devise.IT S.r.l.) <info@2ndquadrant.it>
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
    from setuptools.command.test import test as TestCommand

    class PyTest(TestCommand):
        def finalize_options(self):
            TestCommand.finalize_options(self)
            self.test_args = ['tests']
            self.test_suite = True

        def run_tests(self):
            #import here, cause outside the eggs aren't loaded
            import pytest

            errno = pytest.main(self.test_args)
            sys.exit(errno)
    cmdclass={'test': PyTest}


except ImportError:
    from distutils.core import setup
    cmdclass={}

if sys.version_info < (2, 6):
    raise SystemExit('ERROR: Barman needs at least python 2.6 to work')

install_requires = ['psycopg2', 'argh >= 0.21.2', 'python-dateutil', 'argcomplete']

if sys.version_info < (2, 7):
    install_requires.append('argparse')

barman = {}
with open('barman/version.py', 'r') as fversion:
    exec (fversion.read(), barman)

setup(
    name='barman',
    version=barman['__version__'],
    author='2ndQuadrant Italia (Devise.IT S.r.l.)',
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
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
    ],
    tests_require=['pytest', 'mock', 'pytest-capturelog', 'pytest-timeout'],
    cmdclass=cmdclass,
    use_2to3=True,
)
