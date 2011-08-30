#
# barman - Backup and Recovery Manager for PostgreSQL
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

import psycopg2
from barman.command_wrappers import Command

class Server(object):
    def __init__(self, config):
        self.config = config

    def check_ssh(self):
        cmd = Command("%s -o BatchMode=yes -o StrictHostKeyChecking=no true" % (self.config.ssh_command), shell=True)
        ret = cmd()
        if ret == 0:
            return "\tssh: OK"
        else:
            return "\tssh: FAILED (return code: %s)" % (ret)

    def check_postgres(self):
        try:
            conn = psycopg2.connect(self.config.conninfo)
            cur = conn.cursor()
            cur.execute("SELECT version()")
            version = cur.fetchone()
        except Exception, e:
            return "\tpgsql: FAILED (%s)" % (e.pgcode)
        return "\tpgsql: OK (version: %s)" % (version)

    def check(self):
        yield "Server %s:" % (self.config.name)
        if self.config.description: yield "\tdescription: %s" % (self.config.description)
        yield self.check_ssh()
        yield self.check_postgres()
