# -*- coding: utf-8 -*-
# walrestore - Remote Barman WAL restore command for PostgreSQL
#
# This script remotely fetches WAL files from Barman via SSH, on demand.
# It is intended to be used in restore_command in recovery configuration files
# of PostgreSQL standby servers. Supports parallel fetching and
# protects against SSH failures.
#
# See the help page for usage information.
#
# © Copyright EnterpriseDB UK Limited 2016-2025
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
import os
import shutil
import subprocess
import sys
import time
from io import BytesIO
from multiprocessing import Process

import barman
from barman.compression import CompressionManager, get_server_config_minimal
from barman.utils import force_str

DEFAULT_USER = "barman"
DEFAULT_SPOOL_DIR = "/var/tmp/walrestore"

# The string_types list is used to identify strings
# in a consistent way between python 2 and 3
if sys.version_info[0] == 3:
    string_types = (str,)
else:
    string_types = (basestring,)  # noqa


def main(args=None):
    """
    The main script entry point
    """
    config = parse_arguments(args)

    # Do connectivity test if requested
    if config.test:
        connectivity_test(config)
        return  # never reached

    if config.compression is not None:
        print(
            "WARNING: `%s` option is deprecated and will be removed in future versions. "
            "For WAL compression, please make sure to enable it directly on the Barman "
            "server via the `compression` configuration option" % config.compression
        )

    # Check WAL destination is not a directory
    if os.path.isdir(config.wal_dest):
        exit_with_error(
            "WAL_DEST cannot be a directory: %s" % config.wal_dest, status=3
        )

    # Open the destination file
    try:
        dest_file = open(config.wal_dest, "wb+")
    except EnvironmentError as e:
        exit_with_error(
            "Cannot open '%s' (WAL_DEST) for writing: %s" % (config.wal_dest, e),
            status=3,
        )
        return  # never reached

    # If the file is present in SPOOL_DIR use it and terminate
    try_deliver_from_spool(config, dest_file.name)

    # If requested, load the list of files to fetch in parallel
    additional_files = peek_additional_files(config)

    try:
        # Spawn a process for each additional file to fetch in parallel
        parallel_ssh_processes = spawn_additional_process(config, additional_files)

        try:
            # Execute the main barman get-wal through the ssh connection
            ssh_process = RemoteGetWal(config, config.wal_name, dest_file)
        except EnvironmentError as e:
            exit_with_error('Error executing "ssh": %s' % e, sleep=config.sleep)
            return  # never reached

        # Wait for termination of every parallel process
        for process in parallel_ssh_processes:
            process.join()
    except KeyboardInterrupt:
        # If CTRL+C is pressed, make sure all processes are killed
        for process in parallel_ssh_processes:
            process.kill()
        exit_with_error("SIGINT received! Terminating.")

    # Cleanup failed spool files in case of errors in any of the parallel processes
    for process in parallel_ssh_processes:
        if process.exitcode != 0:
            os.unlink(process.spool_file_name)

    # If the main command succeeded exit here
    if ssh_process.returncode == 0:
        sys.exit(0)

    # Report the exit code, remapping ssh failure code (255) to 2
    if ssh_process.returncode == 255:
        exit_with_error("Connection problem with ssh", 2, sleep=config.sleep)
    else:
        exit_with_error(
            "Remote 'barman get-wal' command has failed!",
            ssh_process.returncode,
            sleep=config.sleep,
        )


class WorkerProcess(Process):
    """
    Class representing a parallel process.

    In essense, this class is the same as ``multiprocessing.Process``,
    but it also keeps track of the spool file name used by the process.
    This is useful for cleaning failed files later in case of errors.
    """

    def __init__(self, spool_file_name, *args, **kwargs):
        super(WorkerProcess, self).__init__(*args, **kwargs)
        self.spool_file_name = spool_file_name


def spawn_additional_process(config, additional_files):
    """
    Execute additional barman get-wal processes

    :param argparse.Namespace config: the configuration from command line
    :param additional_files: A list of WAL file to be downloaded in parallel
    :return list[subprocess.Popen]: list of created processes
    """
    processes = []
    for wal_name in additional_files:
        spool_file_name = os.path.join(config.spool_dir, wal_name)
        process = WorkerProcess(
            target=RemoteGetWal,
            name="RemoteGetWal-%s" % wal_name,
            args=(config, wal_name, spool_file_name),
            kwargs={"is_worker_process": True},
            spool_file_name=spool_file_name,
        )
        process.start()
        processes.append(process)

    return processes


def peek_additional_files(config):
    """
    Invoke remote get-wal --peek to receive a list of wal files to copy

    :param argparse.Namespace config: the configuration from command line
    :returns set: a set of WAL file names from the peek command
    """
    # If parallel downloading is not required return an empty array
    if not config.parallel:
        return []

    # Make sure the SPOOL_DIR exists
    try:
        if not os.path.exists(config.spool_dir):
            os.mkdir(config.spool_dir)
    except EnvironmentError as e:
        exit_with_error("Cannot create '%s' directory: %s" % (config.spool_dir, e))

    # Retrieve the list of files from remote
    additional_files = execute_peek(config)

    # Sanity check
    if len(additional_files) == 0 or additional_files[0] != config.wal_name:
        exit_with_error("The required file is not available: %s" % config.wal_name)

    # Remove the first element, as now we know is identical to config.wal_name
    del additional_files[0]

    return additional_files


def build_ssh_command(config, wal_name, peek=0):
    """
    Prepare an ssh command according to the arguments passed on command line

    :param argparse.Namespace config: the configuration from command line
    :param str wal_name: the wal_name get-wal parameter
    :param int peek: in
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
        ssh_command.append("--config %s" % config.config)

    options = []

    if config.test:
        options.append("--test")
    if peek:
        options.append("--peek '%s'" % peek)
    if config.compression:
        options.append("--%s" % config.compression)
    if config.keep_compression:
        options.append("--keep-compression")
    if config.partial:
        options.append("--partial")

    if options:
        get_wal_command = "get-wal %s '%s' '%s'" % (
            " ".join(options),
            config.server_name,
            wal_name,
        )
    else:
        get_wal_command = "get-wal '%s' '%s'" % (config.server_name, wal_name)

    ssh_command.append(get_wal_command)
    return ssh_command


def execute_peek(config):
    """
    Invoke remote get-wal --peek to receive a list of wal file to copy

    :param argparse.Namespace config: the configuration from command line
    :returns set: a set of WAL file names from the peek command
    """
    # Build the peek command
    ssh_command = build_ssh_command(config, config.wal_name, config.parallel)
    # Issue the command
    try:
        output = subprocess.Popen(ssh_command, stdout=subprocess.PIPE).communicate()
        return list(output[0].decode().splitlines())
    except subprocess.CalledProcessError as e:
        exit_with_error("Impossible to invoke remote get-wal --peek: %s" % e)


def try_deliver_from_spool(config, dest_file):
    """
    Search for the requested file in the spool directory.
    If is already present, then copy it locally and exit,
    return otherwise.

    :param argparse.Namespace config: the configuration from command line
    :param dest_file: The path to the destination file
    """
    spool_file = str(os.path.join(config.spool_dir, config.wal_name))

    # id the file is not present, give up
    if not os.path.exists(spool_file):
        return

    try:
        shutil.move(spool_file, dest_file)
        sys.exit(0)
    except IOError as e:
        exit_with_error("Failure moving %s to %s: %s" % (spool_file, dest_file, e))


def exit_with_error(message, status=2, sleep=0):
    """
    Print ``message`` and terminate the script with ``status``

    :param str message: message to print
    :param int status: script exit code
    :param int sleep: second to sleep before exiting
    """
    print("ERROR: %s" % message, file=sys.stderr)
    # Sleep for config.sleep seconds if required
    if sleep:
        print("Sleeping for %d seconds." % sleep, file=sys.stderr)
        time.sleep(sleep)
    sys.exit(status)


def connectivity_test(config):
    """
    Invoke remote get-wal --test to test the connection with Barman server

    :param argparse.Namespace config: the configuration from command line
    """
    # Build the peek command
    ssh_command = build_ssh_command(config, "dummy_wal_name")
    # Issue the command
    try:
        pipe = subprocess.Popen(
            ssh_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        output = pipe.communicate()
        print(force_str(output[0]))
        sys.exit(pipe.returncode)
    except subprocess.CalledProcessError as e:
        exit_with_error("Impossible to invoke remote get-wal: %s" % e)


def parse_arguments(args=None):
    """
    Parse the command line arguments

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="This script will be used as a 'restore_command' "
        "based on the get-wal feature of Barman. "
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
        "-s",
        "--sleep",
        default=0,
        type=int,
        metavar="SECONDS",
        help="Sleep for SECONDS after a failure of get-wal request. "
        "Defaults to 0 (nowait).",
    )
    parser.add_argument(
        "-p",
        "--parallel",
        default=0,
        type=int,
        metavar="JOBS",
        help="Specifies the number of files to peek and transfer "
        "in parallel. "
        "Defaults to 0 (disabled).",
    )
    parser.add_argument(
        "--spool-dir",
        default=DEFAULT_SPOOL_DIR,
        metavar="SPOOL_DIR",
        help="Specifies spool directory for WAL files. Defaults to "
        "'{0}'.".format(DEFAULT_SPOOL_DIR),
    )
    parser.add_argument(
        "-P",
        "--partial",
        help="retrieve also partial WAL files (.partial)",
        action="store_true",
        dest="partial",
        default=False,
    )
    compression_parser = parser.add_mutually_exclusive_group()
    compression_parser.add_argument(
        "-z",
        "--gzip",
        help="Transfer the WAL files compressed with gzip",
        action="store_const",
        const="gzip",
        dest="compression",
    )
    compression_parser.add_argument(
        "-j",
        "--bzip2",
        help="Transfer the WAL files compressed with bzip2",
        action="store_const",
        const="bzip2",
        dest="compression",
    )
    compression_parser.add_argument(
        "--keep-compression",
        help="Preserve compression during transfer, decompress once received",
        action="store_true",
        dest="keep_compression",
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
        "requested PostgreSQL server in Barman to make sure it is "
        "ready to receive WAL files. With this option, "
        "the 'wal_name' and 'wal_dest' mandatory arguments are ignored.",
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
        "wal_name",
        metavar="WAL_NAME",
        help="The value of the '%%f' keyword (according to 'restore_command').",
    )
    parser.add_argument(
        "wal_dest",
        metavar="WAL_DEST",
        help="The value of the '%%p' keyword (according to 'restore_command').",
    )
    return parser.parse_args(args=args)


class RemoteGetWal(object):
    """
    Class responsible for fetching requested WAL file from the
    remote Barman server via a ``get-wal`` command over ssh.

    If ``--keep-compression`` or one of the compression flags are used,
    the file arrives compressed and is decompressed before being written
    to the destination.
    """

    def __init__(self, config, wal_name, dest_file, is_worker_process=False):
        """
        Spawn a process that download a WAL from remote.

        If needed decompress the remote stream on the fly.

        :param argparse.Namespace config: the configuration from command line
        :param wal_name: The name of WAL to download
        :param dest_file: The destination file name or a writable file object
        :param bool is_worker_process: Whether this is a parallel worker process
        """
        self.config = config
        self.wal_name = wal_name
        self.dest_file = None
        self.ssh_process = None

        # If a string has been passed, it's the name of the destination file
        # We convert it in a writable binary file object
        if isinstance(dest_file, string_types):
            self.dest_file = dest_file
            dest_file = open(dest_file, "wb+")

        # Spawn a remote get-wal process and direct its output to the destination file
        self.ssh_process = subprocess.Popen(
            build_ssh_command(config, wal_name), stdout=dest_file
        )
        self.ssh_process.wait()
        dest_file.seek(0)

        # Identify the WAL compression, if any
        server_config = get_server_config_minimal(config.compression, None)
        compression_manager = CompressionManager(server_config, None)
        compression = compression_manager.identify_compression(dest_file)

        # If compressed, decompress and overwrite the contents of the destination file
        # Note: we are able to use decompress_in_mem here because it's sure that
        # compressor can only be an InternalCompressor
        if compression is not None:
            compressor = compression_manager.get_compressor(compression)
            dec_fileobj = compressor.decompress_in_mem(dest_file)
            dec_fileobj = BytesIO(dec_fileobj.read())  # avoid lazy-decompressors
            dest_file.truncate(0)
            dest_file.seek(0)
            shutil.copyfileobj(dec_fileobj, dest_file)

        # close the opened file
        dest_file.close()

        # If a worker process, exit directly with its return code, allowing the
        # main process to access it via the exitcode attr of the process object
        if is_worker_process:
            sys.exit(self.returncode)

    @property
    def returncode(self):
        """
        Return the exit code of the RemoteGetWal processes.

        :returns: exit code of the RemoteGetWal processe
        """
        return self.ssh_process.returncode


if __name__ == "__main__":
    main()
