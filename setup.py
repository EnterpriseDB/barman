#!/usr/bin/env python
#
# setup.py - setuptools packaging
#
# Copyright (C) 2011  Devise.IT S.r.l. <info@2ndquadrant.it>
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

BaRman is a tool to backup and recovery PostgreSQL clusters. 
"""

BARMAN_VERSION = '0.1.0'

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='barman',
    version=BARMAN_VERSION,
    author='Devise.IT S.r.l.',
    author_email='info@2ndquadrant.it',
    packages=['barman', 'barman.test', ],
    scripts=['bin/barman', ],
    license='GPL-3.0',
    description=__doc__.split("\n")[0],
    long_description="\n".join(__doc__.split("\n")[2:]),
    install_requires=['psycopg2', ],
)
