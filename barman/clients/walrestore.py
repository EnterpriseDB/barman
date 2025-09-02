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
import threading
import time
from tempfile import NamedTemporaryFile

import barman
from barman.compression import (
    CompressionManager,
    InternalCompressor,
    get_server_config_minimal,
)
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
        dest_file = open(config.wal_dest, "wb")
    except EnvironmentError as e:
        exit_with_error(
            "Cannot open '%s' (WAL_DEST) for writing: %s" % (config.wal_dest, e),
            status=3,
        )
        return  # never reached

    # If the file is present in SPOOL_DIR use it and terminate
    try_deliver_from_spool(config, dest_file.name)

    # If required load the list of files to download in parallel
    additional_files = peek_additional_files(config)

    try:
        # Execute barman get-wal through the ssh connection
        ssh_process = RemoteGetWal(config, config.wal_name, dest_file)
    except EnvironmentError as e:
        exit_with_error('Error executing "ssh": %s' % e, sleep=config.sleep)
        return  # never reached

    # Spawn a process for every additional file
    parallel_ssh_processes = spawn_additional_process(config, additional_files)

    # Wait for termination of every subprocess. If CTRL+C is pressed,
    # terminate all of them
    try:
        RemoteGetWal.wait_for_all()
    finally:
        # Cleanup failed spool files in case of errors
        for process in parallel_ssh_processes:
            if process.returncode != 0:
                os.unlink(process.dest_file)

    # If the command succeeded exit here
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
        try:
            # Spawn a process and write the output in the spool dir
            process = RemoteGetWal(config, wal_name, spool_file_name)
            processes.append(process)
        except EnvironmentError:
            # If execution has failed make sure the spool file is unlinked
            try:
                os.unlink(spool_file_name)
            except EnvironmentError:
                # Suppress unlink errors
                pass

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
    threads = set()
    instances = set()
    """
    The list of threads and instances that have been spawned by RemoteGetWal
    """

    def __init__(self, config, wal_name, dest_file):
        """
        Spawn a thread that downloads a WAL from remote.

        If needed decompress the remote stream on the fly.

        :param argparse.Namespace config: the configuration from command line
        :param wal_name: The name of WAL to download
        :param dest_file: The destination file name or a writable file object
        """
        self.config = config
        self.wal_name = wal_name
        self.dest_file_path = None
        self.dest_file_obj = None
        self.ssh_process = None
        self.decompressor_process = None
        self.thread = None
        self.error = None
        self._returncode = None
        self._lock = threading.Lock()

        # If a string has been passed, it's the name of the destination file
        if isinstance(dest_file, string_types):
            self.dest_file_path = dest_file
            self.dest_file_obj = None
        else:
            self.dest_file_obj = dest_file
            self.dest_file_path = None

        # Start the worker thread
        self.thread = threading.Thread(target=self._worker_thread)
        self.thread.daemon = True
        self.thread.start()

        # Register this instance and thread
        self.threads.add(self.thread)
        self.instances.add(self)

    def _worker_thread(self):
        """
        Worker thread that handles SSH process and decompression.
        This runs in the background and properly waits for process completion.
        """
        source_file = None
        dest_file = None

        try:
            # Create temporary file for SSH output
            source_file = NamedTemporaryFile(mode="r+b", prefix=".%s." % os.path.basename(self.wal_name))

            # Open destination file if we have a path
            if self.dest_file_path:
                dest_file = open(self.dest_file_path, "wb")
            else:
                dest_file = self.dest_file_obj

            # Start SSH process
            self.ssh_process = subprocess.Popen(
                build_ssh_command(self.config, self.wal_name),
                stdout=source_file
            )

            # Wait for SSH process to complete
            self.ssh_process.wait()

            # Reset file pointer to beginning
            source_file.seek(0)

            # Identify WAL compression, if any
            server_config = get_server_config_minimal(self.config.compression, None)
            compression_manager = CompressionManager(server_config, None)
            compression = compression_manager.identify_compression(source_file.name)

            # Process the file based on compression
            if compression is None:
                # No compression - direct copy
                shutil.copyfileobj(source_file, dest_file)
            else:
                # Handle compression
                compressor = compression_manager.get_compressor(compression)
                if isinstance(compressor, InternalCompressor):
                    # Use internal decompressor
                    compressor.decompress(source_file.name, dest_file.name)
                else:
                    # Use external decompressor
                    source_file.seek(0)  # Reset for decompressor input
                    self.decompressor_process = subprocess.Popen(
                        [compression, "-d"],
                        stdin=source_file,
                        stdout=dest_file,
                    )
                    # Wait for decompressor to complete
                    self.decompressor_process.wait()

            # Set successful return code
            with self._lock:
                self._returncode = self._calculate_return_code()

        except Exception as e:
            # Store error for later retrieval
            self.error = e
            with self._lock:
                self._returncode = 1

        finally:
            # Clean up file handles
            if dest_file and self.dest_file_path:
                dest_file.close()
            if source_file:
                source_file.close()

    def _calculate_return_code(self):
        """Calculate the overall return code based on subprocess return codes."""
        if self.ssh_process and self.ssh_process.returncode != 0:
            return self.ssh_process.returncode
        if self.decompressor_process and self.decompressor_process.returncode != 0:
            return self.decompressor_process.returncode
        return 0

    def _wait_for_header_bytes(self, source_file, min_bytes=20, timeout=5.0):
        """
        Wait for minimum bytes to be available in source file.
        This can be used for early compression detection.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                size = os.path.getsize(source_file.name)
                if size >= min_bytes:
                    return True
                if self.ssh_process and self.ssh_process.poll() is not None:
                    return size > 0  # Process finished, return True if we got any data
            except OSError:
                pass  # File might not exist yet
            time.sleep(0.01)
        return False  # Timeout

    @classmethod
    def wait_for_all(cls):
        """
        Wait for the termination of all the registered threads.
        """
        try:
            # Wait for all threads to complete
            for thread in cls.threads.copy():
                if thread.is_alive():
                    thread.join()

            # Clear the collections after all threads are done
            cls.threads.clear()
            cls.instances.clear()

        except KeyboardInterrupt:
            # If a SIGINT has been received, make sure that every subprocess terminates
            for instance in cls.instances:
                if instance.ssh_process:
                    instance.ssh_process.kill()
                if instance.decompressor_process:
                    instance.decompressor_process.kill()
            exit_with_error("SIGINT received! Terminating.")

    @property
    def returncode(self):
        """
        Return the exit code of the RemoteGetWal processes.

        A remote get-wal process return code is 0 only if both the remote
        get-wal process and the eventual decompressor return 0

        :return: exit code of the RemoteGetWal processes
        """
        # Wait for thread to complete if still running
        if self.thread and self.thread.is_alive():
            self.thread.join()

        # Return the calculated return code
        with self._lock:
            if self._returncode is not None:
                return self._returncode
            elif self.error:
                return 1
            else:
                # Thread hasn't finished yet or no return code set
                return None

    def get_error(self):
        """
        Return any error that occurred during execution.

        :return: Exception object if an error occurred, None otherwise
        """
        return self.error


if __name__ == "__main__":
    main()
