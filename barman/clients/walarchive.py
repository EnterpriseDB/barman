# -*- coding: utf-8 -*-
# walarchive - Remote Barman WAL archive command for PostgreSQL
#
# This script remotely sends WAL files to Barman via SSH, on demand.
# It is intended to be used as archive_command in PostgreSQL configuration.
#
# See the help page for usage information.
#
# © Copyright EnterpriseDB UK Limited 2019-2022
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

from __future__ import print_function

import argparse
import copy
import hashlib
import os
import subprocess
import sys
import tarfile
import time
from contextlib import closing
from io import BytesIO

import barman

DEFAULT_USER = "barman"
BUFSIZE = 16 * 1024


def main(args=None):
    """
    The main script entry point

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    """
    config = parse_arguments(args)

    # Do connectivity test if requested
    if config.test:
        connectivity_test(config)
        return  # never reached

    # Check WAL destination is not a directory
    if os.path.isdir(config.wal_path):
        exit_with_error("WAL_PATH cannot be a directory: %s" % config.wal_path)

    try:
        # Execute barman put-wal through the ssh connection
        ssh_process = RemotePutWal(config, config.wal_path)
    except EnvironmentError as exc:
        exit_with_error("Error executing ssh: %s" % exc)
        return  # never reached

    # Wait for termination of every subprocess. If CTRL+C is pressed,
    # terminate all of them
    RemotePutWal.wait_for_all()

    # If the command succeeded exit here
    if ssh_process.returncode == 0:
        return

    # Report the exit code, remapping ssh failure code (255) to 3
    if ssh_process.returncode == 255:
        exit_with_error("Connection problem with ssh", 3)
    else:
        exit_with_error(
            "Remote 'barman put-wal' command has failed!", ssh_process.returncode
        )


def build_ssh_command(config):
    """
    Prepare an ssh command according to the arguments passed on command line

    :param argparse.Namespace config: the configuration from command line
    :return list[str]: the ssh command as list of string
    """
    ssh_command = ["ssh"]
    if config.port is not None:
        ssh_command += ["-p", config.port]
    ssh_command += [
        "-q",  # quiet mode - suppress warnings
        "-T",  # disable pseudo-terminal allocation
        "%s@%s" % (config.user, config.barman_host),
        "barman",
    ]

    if config.config:
        ssh_command.append("--config='%s'" % config.config)

    ssh_command.extend(["put-wal", config.server_name])

    if config.test:
        ssh_command.append("--test")

    return ssh_command


def exit_with_error(message, status=2):
    """
    Print ``message`` and terminate the script with ``status``

    :param str message: message to print
    :param int status: script exit code
    """
    print("ERROR: %s" % message, file=sys.stderr)
    sys.exit(status)


def connectivity_test(config):
    """
    Invoke remote put-wal --test to test the connection with Barman server

    :param argparse.Namespace config: the configuration from command line
    """
    ssh_command = build_ssh_command(config)
    try:
        pipe = subprocess.Popen(
            ssh_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        output = pipe.communicate()
        print(output[0].decode("utf-8"))
        sys.exit(pipe.returncode)
    except subprocess.CalledProcessError as e:
        exit_with_error("Impossible to invoke remote put-wal: %s" % e)


def parse_arguments(args=None):
    """
    Parse the command line arguments

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="This script will be used as an 'archive_command' "
        "based on the put-wal feature of Barman. "
        "A ssh connection will be opened to the Barman host.",
    )
    parser.add_argument(
        "-V", "--version", action="version", version="%%(prog)s %s" % barman.__version__
    )
    parser.add_argument(
        "-U",
        "--user",
        default=DEFAULT_USER,
        help="The user used for the ssh connection to the Barman server. "
        "Defaults to '%(default)s'.",
    )
    parser.add_argument(
        "--port",
        help="The port used for the ssh connection to the Barman server.",
    )
    parser.add_argument(
        "-c",
        "--config",
        metavar="CONFIG",
        help="configuration file on the Barman server",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="test both the connection and the configuration of the "
        "requested PostgreSQL server in Barman for WAL retrieval. "
        "With this option, the 'wal_name' mandatory argument is "
        "ignored.",
    )
    parser.add_argument(
        "barman_host",
        metavar="BARMAN_HOST",
        help="The host of the Barman server.",
    )
    parser.add_argument(
        "server_name",
        metavar="SERVER_NAME",
        help="The server name configured in Barman from which WALs are taken.",
    )
    parser.add_argument(
        "wal_path",
        metavar="WAL_PATH",
        help="The value of the '%%p' keyword (according to 'archive_command').",
    )
    return parser.parse_args(args=args)


def md5copyfileobj(src, dst, length=None):
    """
    Copy length bytes from fileobj src to fileobj dst.
    If length is None, copy the entire content.
    This method is used by the ChecksumTarFile.addfile().
    Returns the md5 checksum
    """
    checksum = hashlib.md5()
    if length == 0:
        return checksum.hexdigest()

    if length is None:
        while 1:
            buf = src.read(BUFSIZE)
            if not buf:
                break
            checksum.update(buf)
            dst.write(buf)
        return checksum.hexdigest()

    blocks, remainder = divmod(length, BUFSIZE)
    for _ in range(blocks):
        buf = src.read(BUFSIZE)
        if len(buf) < BUFSIZE:
            raise IOError("end of file reached")
        checksum.update(buf)
        dst.write(buf)

    if remainder != 0:
        buf = src.read(remainder)
        if len(buf) < remainder:
            raise IOError("end of file reached")
        checksum.update(buf)
        dst.write(buf)
    return checksum.hexdigest()


class ChecksumTarInfo(tarfile.TarInfo):
    """
    Special TarInfo that can hold a file checksum
    """

    def __init__(self, *args, **kwargs):
        super(ChecksumTarInfo, self).__init__(*args, **kwargs)
        self.data_checksum = None


class ChecksumTarFile(tarfile.TarFile):
    """
    Custom TarFile class that automatically calculates md5 checksum
    of each file and appends a file called 'MD5SUMS' to the stream.
    """

    tarinfo = ChecksumTarInfo  # The default TarInfo class used by TarFile

    format = tarfile.PAX_FORMAT  # Use PAX format to better preserve metadata

    MD5SUMS_FILE = "MD5SUMS"

    def addfile(self, tarinfo, fileobj=None):
        """
        Add the provided fileobj to the tar using md5copyfileobj
        and saves the file md5 in the provided ChecksumTarInfo object.

        This method completely replaces TarFile.addfile()
        """
        self._check("aw")

        tarinfo = copy.copy(tarinfo)

        buf = tarinfo.tobuf(self.format, self.encoding, self.errors)
        self.fileobj.write(buf)
        self.offset += len(buf)

        # If there's data to follow, append it.
        if fileobj is not None:
            tarinfo.data_checksum = md5copyfileobj(fileobj, self.fileobj, tarinfo.size)
            blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
            if remainder > 0:
                self.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                blocks += 1
            self.offset += blocks * tarfile.BLOCKSIZE

        self.members.append(tarinfo)

    def close(self):
        """
        Add an MD5SUMS file to the tar just before closing.

        This method extends TarFile.close().
        """
        if self.closed:
            return

        if self.mode in "aw":
            with BytesIO() as md5sums:
                for tarinfo in self.members:
                    line = "%s *%s\n" % (tarinfo.data_checksum, tarinfo.name)
                    md5sums.write(line.encode())
                md5sums.seek(0, os.SEEK_END)
                size = md5sums.tell()
                md5sums.seek(0, os.SEEK_SET)
                tarinfo = self.tarinfo(self.MD5SUMS_FILE)
                tarinfo.size = size
                self.addfile(tarinfo, md5sums)

        super(ChecksumTarFile, self).close()


class RemotePutWal(object):
    """
    Spawn a process that sends a WAL to a remote Barman server.

    :param argparse.Namespace config: the configuration from command line
    :param wal_path: The name of WAL to upload
    """

    processes = set()
    """
    The list of processes that has been spawned by RemotePutWal
    """

    def __init__(self, config, wal_path):
        self.config = config
        self.wal_path = wal_path
        self.dest_file = None

        # Spawn a remote put-wal process
        self.ssh_process = subprocess.Popen(
            build_ssh_command(config), stdin=subprocess.PIPE
        )

        # Register the spawned processes in the class registry
        self.processes.add(self.ssh_process)

        # Send the data as a tar file (containing checksums)
        with self.ssh_process.stdin as dest_file:
            with closing(ChecksumTarFile.open(mode="w|", fileobj=dest_file)) as tar:
                tar.add(wal_path, os.path.basename(wal_path))

    @classmethod
    def wait_for_all(cls):
        """
        Wait for the termination of all the registered spawned processes.
        """
        try:
            while cls.processes:
                time.sleep(0.1)
                for process in cls.processes.copy():
                    if process.poll() is not None:
                        cls.processes.remove(process)
        except KeyboardInterrupt:
            # If a SIGINT has been received, make sure that every subprocess
            # terminate
            for process in cls.processes:
                process.kill()
            exit_with_error("SIGINT received! Terminating.")

    @property
    def returncode(self):
        """
        Return the exit code of the RemoteGetWal processes.

        :return: exit code of the RemoteGetWal processes
        """
        if self.ssh_process.returncode != 0:
            return self.ssh_process.returncode
        return 0


if __name__ == "__main__":
    main()
