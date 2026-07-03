# -*- coding: utf-8 -*-
# walarchive - Remote Barman WAL archive command for PostgreSQL
#
# This script remotely sends WAL files to Barman via SSH, on demand.
# It is intended to be used as archive_command in PostgreSQL configuration.
#
# See the help page for usage information.
#
# © Copyright EnterpriseDB UK Limited 2019-2026
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
from contextlib import closing
from io import BytesIO
from tempfile import TemporaryDirectory

import barman
from barman.compression import get_internal_compressor
from barman.config import parse_compression_level
from barman.exceptions import ArchiverFailure
from barman.wal_archiver import ParallelWalArchiver

DEFAULT_USER = "barman"
BUFSIZE = 16 * 1024
PREFETCH_CACHE_DIR = "/tmp/barman-{server_name}-{computed_hash}"


def _default_prefetch_cache_dir(config):
    """
    Compute the default prefetch cache directory for a given configuration.

    The cache records which WALs have already been archived, so it must be scoped
    to the specific Barman destination (host, port and config file) as well as to
    the server name.

    :param argparse.Namespace config: the configuration from command line.
    :return: the default cache directory path for this configuration.
    :rtype: str
    """
    destination = "%s:%s:%s" % (config.barman_host, config.port, config.config)
    computed_hash = hashlib.sha256(destination.encode()).hexdigest()[:16]
    return PREFETCH_CACHE_DIR.format(
        server_name=config.server_name, computed_hash=computed_hash
    )


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
        prefetch_cache_dir = config.prefetch_cache_dir or _default_prefetch_cache_dir(
            config
        )
        archiver = RemoteWalArchiver(config.server_name, prefetch_cache_dir, config)
        archiver.archive(config.wal_path, parallel=config.parallel)
    except EnvironmentError as exc:
        exit_with_error("Error executing ssh: %s" % exc)
        return  # never reached
    except ArchiverFailure:
        # Something went wrong with the archival. The exit code is already set in
        # archiver.main_wal_returncode, so we can pass and let it be treated below.
        pass

    # If the WAL file was found in the cache, we can exit with success
    if archiver.found_in_cache:
        return

    # If the requested WAL succeeded, we can exit with success
    if archiver.main_wal_returncode == 0:
        return

    # Report the exit code, remapping ssh failure code (255) to 3
    if archiver.main_wal_returncode == 255:
        exit_with_error("Connection problem with ssh", 3)
    elif archiver.main_wal_returncode is not None:
        exit_with_error(
            "Remote 'barman put-wal' command has failed!", archiver.main_wal_returncode
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
        "--md5",
        action="store_true",
        help="Use MD5 as the hash algorithm to maintain compatibility between "
        "mismatched client and server versions.",
    )
    parser.add_argument(
        "-z",
        "--gzip",
        help="gzip-compress the WAL file before sending it",
        action="store_const",
        const="gzip",
        dest="compression",
    )
    parser.add_argument(
        "-j",
        "--bzip2",
        help="bzip2-compress the WAL file before sending it",
        action="store_const",
        const="bzip2",
        dest="compression",
    )
    parser.add_argument(
        "--xz",
        help="xz-compress the WAL file before sending it",
        action="store_const",
        const="xz",
        dest="compression",
    )
    parser.add_argument(
        "--snappy",
        help="snappy-compress the WAL file before sending it "
        "(requires optional python-snappy library)",
        action="store_const",
        const="snappy",
        dest="compression",
    )
    parser.add_argument(
        "--zstd",
        help="zstd-compress the WAL file before sending it "
        "(requires optional zstandard library)",
        action="store_const",
        const="zstd",
        dest="compression",
    )
    parser.add_argument(
        "--lz4",
        help="lz4-compress the WAL file before sending it "
        "(requires optional lz4 library)",
        action="store_const",
        const="lz4",
        dest="compression",
    )
    parser.add_argument(
        "--compression-level",
        help="A compression level for the specified compression algorithm",
        dest="compression_level",
        type=parse_compression_level,
        default=None,
    )
    parser.add_argument(
        "-p",
        "--parallel",
        default=0,
        type=int,
        help="Specifies the maximum number of WALs to archive in parallel",
    )
    parser.add_argument(
        "--prefetch-cache-dir",
        metavar="PREFETCH_CACHE_DIR",
        help="Directory where to store cache metadata when using --parallel. "
        "Defaults to '%s'."
        % PREFETCH_CACHE_DIR.format(
            server_name="SERVER_NAME", computed_hash="COMPUTED_HASH"
        ),
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


def hashCopyfileobj(src, dst, length=None, hash_algorithm="sha256"):
    """
    Copy length bytes from fileobj src to fileobj dst.
    If length is None, copy the entire content.
    This method is used by the ChecksumTarFile.addfile().
    Returns the checksum for the specified hashing algorithm.
    """
    checksum = hashlib.new(hash_algorithm)
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
    Custom TarFile class that automatically calculates hash checksum
    of each file and appends a file called 'MD5SUMS' or 'SHA256SUMS' to the stream,
    depending on the hash algorithm specified.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a ``ChecksumTarFile`` instance.

        This constructor behaves like ``tarfile.TarFile``, but adds two attributes.
        ``hash_algorithm`` is the hashing algorithm used for checksums (default is
        ``sha256``).``HASHSUMS_FILE`` is the name of the file where checksums will be stored
        (default is ``SHA256SUMS``).
        """
        super(ChecksumTarFile, self).__init__(*args, **kwargs)
        self.hash_algorithm = "sha256"
        self.HASHSUMS_FILE = "SHA256SUMS"

    tarinfo = ChecksumTarInfo  # The default TarInfo class used by TarFile

    format = tarfile.PAX_FORMAT  # Use PAX format to better preserve metadata

    def addfile(self, tarinfo, fileobj=None):
        """
        Add the provided fileobj to the tar using hashCopyfileobj
        and saves the file hash in the provided ChecksumTarInfo object.

        This method completely replaces TarFile.addfile()
        """
        self._check("aw")

        tarinfo = copy.copy(tarinfo)

        buf = tarinfo.tobuf(self.format, self.encoding, self.errors)
        self.fileobj.write(buf)
        self.offset += len(buf)

        # If there's data to follow, append it.
        if fileobj is not None:
            tarinfo.data_checksum = hashCopyfileobj(
                fileobj, self.fileobj, tarinfo.size, self.hash_algorithm
            )
            blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
            if remainder > 0:
                self.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                blocks += 1
            self.offset += blocks * tarfile.BLOCKSIZE
        self.members.append(tarinfo)

    def close(self):
        """
        Add a :attr:`HASHSUMS_FILE` file to the tar just before closing.

        This method extends TarFile.close().
        """
        if self.closed:
            return

        if self.mode in "aw":
            with BytesIO() as hashsums:
                for tarinfo in self.members:
                    line = "%s *%s\n" % (tarinfo.data_checksum, tarinfo.name)
                    hashsums.write(line.encode())
                hashsums.seek(0, os.SEEK_END)
                size = hashsums.tell()
                hashsums.seek(0, os.SEEK_SET)
                tarinfo = self.tarinfo(self.HASHSUMS_FILE)
                tarinfo.size = size
                self.addfile(tarinfo, hashsums)

        super(ChecksumTarFile, self).close()


class RemotePutWal(object):
    """
    Spawn an SSH process that sends a WAL to a remote Barman server.

    :param argparse.Namespace config: the configuration from command line
    :param wal_path: The name of WAL to upload
    """

    def __init__(self, config, wal_path):
        self.config = config
        self.wal_path = wal_path
        self.dest_file = None

        # Spawn a remote put-wal process
        self.ssh_process = subprocess.Popen(
            build_ssh_command(config), stdin=subprocess.PIPE
        )

        # Check if md5 flag was used.
        hash_settings = {True: ("md5", "MD5SUMS"), False: ("sha256", "SHA256SUMS")}
        hash_algorithm, HASHSUMS_FILE = hash_settings[config.md5]

        # Send the data as a tar file (containing checksums)
        with self.ssh_process.stdin as dest_file:
            with closing(ChecksumTarFile.open(mode="w|", fileobj=dest_file)) as tar:
                filename = os.path.basename(wal_path)
                tar.hash_algorithm = hash_algorithm
                tar.HASHSUMS_FILE = HASHSUMS_FILE
                if config.compression is not None:
                    with TemporaryDirectory(prefix="barman-wal-archive-") as tmpdir:
                        compressor = get_internal_compressor(
                            config.compression, config.compression_level
                        )
                        compressed_file_path = os.path.join(tmpdir, filename)
                        compressor.compress(wal_path, compressed_file_path)
                        tar.add(compressed_file_path, filename)
                else:
                    tar.add(wal_path, filename)

    def wait(self):
        """
        Wait for the termination of the SSH process.
        """
        try:
            return self.ssh_process.wait()
        except KeyboardInterrupt:
            self.ssh_process.kill()
            exit_with_error("SIGINT received! Terminating.")

    @property
    def returncode(self):
        """
        Return the exit code of the RemotePutWal processes.

        :return: exit code of the RemotePutWal processes
        """
        return self.ssh_process.returncode


class RemoteWalArchiver(ParallelWalArchiver):
    def __init__(self, server_name, cache_dir, config):
        """
        Initialize a RemoteWalArchiver instance.

        :param str server_name: The name of the Barman server.
        :param str cache_dir: Directory where the last-archived cache file is stored.
        :param argparse.Namespace config: The CLI configuration from command line.
        """
        super().__init__(server_name, cache_dir)
        self.config = config
        self.found_in_cache = False
        self.main_wal_returncode = None

    def _is_already_archived(self, wal_path):
        """
        Check if the WAL file has already been archived.

        This method only overrides the parent class method to store the result in the
        ``found_in_cache`` attribute so that it can be accessed later in
        :func:`main()`.

        :param str wal_path: The path of the WAL file to check.
        :return: True if the WAL file is already archived, False otherwise.
        """
        ret = super()._is_already_archived(wal_path)
        self.found_in_cache = ret
        return ret

    def _archive_single_wal(self, wal_path):
        """
        Archive a single WAL file to the remote Barman server.

        :param wal_path: The name of WAL to upload
        """
        put_wal = RemotePutWal(self.config, wal_path)
        put_wal.wait()

        # Store the return code of the main WAL SSH process so it can be later checked
        # in main() to determine the exit code of the script.
        if wal_path == self.config.wal_path:
            self.main_wal_returncode = put_wal.returncode

        # Raise an exception if the SSH process was not successful. This is important so
        # that ParallelWalArchiver can determine if the archival of this WAL was successful
        # or not, which affects what gets written in the cache file.
        if put_wal.returncode != 0:
            raise ArchiverFailure(
                "Remote 'barman put-wal' command has failed! (exit code %s)"
                % put_wal.returncode
            )


if __name__ == "__main__":
    main()
