# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2025
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

import logging
import re
import shutil
import sys
from abc import ABCMeta, abstractmethod

from barman import output
from barman.command_wrappers import Command, full_command_quote
from barman.exceptions import FsOperationFailed
from barman.utils import with_metaclass

_logger = logging.getLogger(__name__)


class UnixLocalCommand(object):
    """
    This class is a wrapper for local calls for file system operations
    """

    def __init__(self, path=None):
        # initialize a shell
        self.internal_cmd = Command(cmd="sh", args=["-c"], path=path)

    def cmd(self, cmd_name, args=[]):
        """
        Execute a command string, escaping it, if necessary
        """
        return self.internal_cmd(full_command_quote(cmd_name, args))

    def get_last_output(self):
        """
        Return the output and the error strings from the last executed command

        :rtype: tuple[str,str]
        """
        return self.internal_cmd.out, self.internal_cmd.err

    def move(self, source_path, dest_path):
        """
        Move a file from source_path to dest_path.

        :param str source_path: full path to the source file.
        :param str dest_path: full path to the destination file.
        :returns bool: True if the move completed successfully,
            False otherwise.
        """
        _logger.debug("Moving %s to %s" % (source_path, dest_path))
        mv_ret = self.cmd("mv", args=[source_path, dest_path])
        if mv_ret == 0:
            return True
        else:
            raise FsOperationFailed("mv execution failed")

    def create_dir_if_not_exists(self, dir_path, mode=None):
        """
        This method recursively creates a directory if not exists

        If the path exists and is not a directory raise an exception.

        :param str dir_path: full path for the directory
        :param mode str|None: Specify the mode to use for creation. Not used
            if the directory already exists.
        :returns bool: False if the directory already exists True if the
            directory is created.
        """
        _logger.debug("Create directory %s if it does not exists" % dir_path)
        if self.check_directory_exists(dir_path):
            return False
        else:
            # Make parent directories if needed
            args = ["-p", dir_path]
            if mode is not None:
                args.extend(["-m", mode])
            mkdir_ret = self.cmd("mkdir", args=args)
            if mkdir_ret == 0:
                return True
            else:
                raise FsOperationFailed("mkdir execution failed")

    def delete_if_exists(self, path):
        """
        This method check for the existence of a path.
        If it exists, then is removed using a rm -fr command,
        and returns True.
        If the command fails an exception is raised.
        If the path does not exists returns False

        :param path the full path for the directory
        """
        _logger.debug("Delete path %s if exists" % path)
        exists = self.exists(path, False)
        if exists:
            rm_ret = self.cmd("rm", args=["-fr", path])
            if rm_ret == 0:
                return True
            else:
                raise FsOperationFailed("rm execution failed")
        else:
            return False

    def check_directory_exists(self, dir_path):
        """
        Check for the existence of a directory in path.
        if the directory exists returns true.
        if the directory does not exists returns false.
        if exists a file and is not a directory raises an exception

        :param dir_path full path for the directory
        """
        _logger.debug("Check if directory %s exists" % dir_path)
        exists = self.exists(dir_path)
        if exists:
            is_dir = self.cmd("test", args=["-d", dir_path])
            if is_dir != 0:
                raise FsOperationFailed(
                    "A file with the same name exists, but is not a directory"
                )
            else:
                return True
        else:
            return False

    def get_file_mode(self, path):
        """
        Should check that
        :param dir_path:
        :param mode:
        :return: mode
        """
        if not self.exists(path):
            raise FsOperationFailed("Following path does not exist: %s" % path)
        args = ["-c", "%a", path]
        if self.is_osx():
            print("is osx")
            args = ["-f", "%Lp", path]
        cmd_ret = self.cmd("stat", args=args)
        if cmd_ret != 0:
            raise FsOperationFailed(
                "Failed to get file mode for %s: %s" % (path, self.internal_cmd.err)
            )
        return self.internal_cmd.out.strip()

    def is_osx(self):
        """
        Identify whether is is a Linux or Darwin system
        :return: True is it is osx os
        """
        self.cmd("uname", args=["-s"])
        if self.internal_cmd.out.strip() == "Darwin":
            return True
        return False

    def validate_file_mode(self, path, mode):
        """
        Validate the file or dir has the expected mode. Raises an exception otherwise.
        :param path: str
        :param mode: str (700, 750, ...)
        :return:
        """
        path_mode = self.get_file_mode(path)
        if path_mode != mode:
            FsOperationFailed(
                "Following file %s does not have expected access right %s. Got %s instead"
                % (path, mode, path_mode)
            )

    def check_write_permission(self, dir_path):
        """
        check write permission for barman on a given path.
        Creates a hidden file using touch, then remove the file.
        returns true if the file is written and removed without problems
        raise exception if the creation fails.
        raise exception if the removal fails.

        :param dir_path full dir_path for the directory to check
        """
        _logger.debug("Check if directory %s is writable" % dir_path)
        exists = self.exists(dir_path)
        if exists:
            is_dir = self.cmd("test", args=["-d", dir_path])
            if is_dir == 0:
                can_write = self.cmd(
                    "touch", args=["%s/.barman_write_check" % dir_path]
                )
                if can_write == 0:
                    can_remove = self.cmd(
                        "rm", args=["%s/.barman_write_check" % dir_path]
                    )
                    if can_remove == 0:
                        return True
                    else:
                        raise FsOperationFailed("Unable to remove file")
                else:
                    raise FsOperationFailed("Unable to create write check file")
            else:
                raise FsOperationFailed("%s is not a directory" % dir_path)
        else:
            raise FsOperationFailed("%s does not exists" % dir_path)

    def create_symbolic_link(self, src, dst):
        """
        Create a symlink pointing to src named dst.
        Check src exists, if so, checks that destination
        does not exists. if src is an invalid folder, raises an exception.
        if dst already exists, raises an exception. if ln -s command fails
        raises an exception

        :param src full path to the source of the symlink
        :param dst full path for the destination of the symlink
        """
        _logger.debug("Create symbolic link %s -> %s" % (dst, src))
        exists = self.exists(src)
        if exists:
            exists_dst = self.exists(dst)
            if not exists_dst:
                link = self.cmd("ln", args=["-s", src, dst])
                if link == 0:
                    return True
                else:
                    raise FsOperationFailed("ln command failed")
            else:
                raise FsOperationFailed("ln destination already exists")
        else:
            raise FsOperationFailed("ln source does not exists")

    def get_system_info(self):
        """
        Gather important system information for 'barman diagnose' command
        """
        result = {}
        # self.internal_cmd.out can be None. The str() call will ensure it
        # will be translated to a literal 'None'
        release = ""
        if self.cmd("lsb_release", args=["-a"]) == 0:
            release = self.internal_cmd.out.rstrip()
        elif self.exists("/etc/lsb-release"):
            self.cmd("cat", args=["/etc/lsb-release"])
            release = "Ubuntu Linux %s" % self.internal_cmd.out.rstrip()
        elif self.exists("/etc/debian_version"):
            self.cmd("cat", args=["/etc/debian_version"])
            release = "Debian GNU/Linux %s" % self.internal_cmd.out.rstrip()
        elif self.exists("/etc/redhat-release"):
            self.cmd("cat", args=["/etc/redhat-release"])
            release = "RedHat Linux %s" % self.internal_cmd.out.rstrip()
        elif self.cmd("sw_vers") == 0:
            release = self.internal_cmd.out.rstrip()
        result["release"] = release

        self.cmd("uname", args=["-a"])
        result["kernel_ver"] = self.internal_cmd.out.rstrip()
        result["python_ver"] = "Python %s.%s.%s" % (
            sys.version_info.major,
            sys.version_info.minor,
            sys.version_info.micro,
        )
        result["python_executable"] = sys.executable
        self.cmd("rsync", args=["--version", "2>&1"])
        try:
            result["rsync_ver"] = self.internal_cmd.out.splitlines(True)[0].rstrip()
        except IndexError:
            result["rsync_ver"] = ""
        self.cmd("ssh", args=["-V", "2>&1"])
        result["ssh_ver"] = self.internal_cmd.out.rstrip()
        return result

    def get_file_content(self, path):
        """
        Retrieve the content of a file
        If the file doesn't exist or isn't readable, it raises an exception.

        :param str path: full path to the file to read
        """
        _logger.debug("Reading content of file %s" % path)

        result = self.exists(path)
        if not result:
            raise FsOperationFailed("The %s file does not exist" % path)

        result = self.cmd("test", args=["-r", path])
        if result != 0:
            raise FsOperationFailed("The %s file is not readable" % path)

        result = self.cmd("cat", args=[path])
        if result != 0:
            raise FsOperationFailed("Failed to execute \"cat '%s'\"" % path)

        return self.internal_cmd.out

    def exists(self, path, dereference=True):
        """
        Check for the existence of a path.

        :param str path: full path to check
        :param bool dereference: whether dereference symlinks, defaults
            to True
        :return bool: if the file exists or not.
        """
        _logger.debug("check for existence of: %s" % path)
        options = ["-e", path]
        if not dereference:
            options += ["-o", "-L", path]
        result = self.cmd("test", args=options)
        return result == 0

    def ping(self):
        """
        'Ping' the server executing the `true` command.

        :return int: the true cmd result
        """
        _logger.debug("execute the true command")
        result = self.cmd("true")
        return result

    def list_dir_content(self, dir_path, options=[]):
        """
        List the contents of a given directory.

        :param str dir_path: the path where we want the ls to be executed
        :param list[str] options: a string containing the options for the ls
            command
        :return str: the ls cmd output
        """
        _logger.debug("list the content of a directory")
        ls_options = []
        if options:
            ls_options += options
        ls_options.append(dir_path)
        self.cmd("ls", args=ls_options)
        return self.internal_cmd.out

    def findmnt(self, device):
        """
        Retrieve the mount point and mount options for the provided device.

        :param str device: The device for which the mount point and options should
            be found.
        :rtype: List[str|None, str|None]
        :return: The mount point and the mount options of the specified device or
            [None, None] if the device could not be found by findmnt.
        """
        _logger.debug("finding mount point and options for device %s", device)
        self.cmd("findmnt", args=("-o", "TARGET,OPTIONS", "-n", device))
        output = self.internal_cmd.out
        if output == "":
            # No output means we successfully ran the command but couldn't find
            # the mount point
            return [None, None]
        output_fields = output.split()
        if len(output_fields) != 2:
            raise FsOperationFailed(
                "Unexpected findmnt output: %s" % self.internal_cmd.out
            )
        else:
            return output_fields


class UnixRemoteCommand(UnixLocalCommand):
    """
    This class is a wrapper for remote calls for file system operations
    """

    # noinspection PyMissingConstructor
    def __init__(self, ssh_command, ssh_options=None, path=None):
        """
        Uses the same commands as the UnixLocalCommand
        but the constructor is overridden and a remote shell is
        initialized using the ssh_command provided by the user

        :param str ssh_command: the ssh command provided by the user
        :param list[str] ssh_options: the options to be passed to SSH
        :param str path: the path to be used if provided, otherwise
          the PATH environment variable will be used
        """
        # Ensure that ssh_option is iterable
        if ssh_options is None:
            ssh_options = []

        if ssh_command is None:
            raise FsOperationFailed("No ssh command provided")
        self.internal_cmd = Command(
            ssh_command, args=ssh_options, path=path, shell=True
        )
        try:
            ret = self.cmd("true")
        except OSError:
            raise FsOperationFailed("Unable to execute %s" % ssh_command)
        if ret != 0:
            raise FsOperationFailed(
                "Connection failed using '%s %s' return code %s"
                % (ssh_command, " ".join(ssh_options), ret)
            )


def unix_command_factory(remote_command=None, path=None):
    """
    Function in charge of instantiating a Unix Command.

    :param remote_command:
    :param path:
    :return: UnixLocalCommand
    """
    if remote_command:
        try:
            cmd = UnixRemoteCommand(remote_command, path=path)
            logging.debug("Created a UnixRemoteCommand")
            return cmd
        except FsOperationFailed:
            output.error(
                "Unable to connect to the target host using the command '%s'",
                remote_command,
            )
            output.close_and_exit()
    else:
        cmd = UnixLocalCommand()
        logging.debug("Created a UnixLocalCommand")
        return cmd


def path_allowed(exclude, include, path, is_dir):
    """
    Filter files based on include/exclude lists.

    The rules are evaluated in steps:

    1. if there are include rules and the proposed path match them, it
       is immediately accepted.

    2. if there are exclude rules and the proposed path match them, it
       is immediately rejected.

    3. the path is accepted.

    Look at the documentation for the "evaluate_path_matching_rules" function
    for more information about the syntax of the rules.

    :param list[str]|None exclude: The list of rules composing the exclude list
    :param list[str]|None include: The list of rules composing the include list
    :param str path: The patch to patch
    :param bool is_dir: True is the passed path is a directory
    :return bool: True is the patch is accepted, False otherwise
    """
    if include and _match_path(include, path, is_dir):
        return True
    if exclude and _match_path(exclude, path, is_dir):
        return False
    return True


def _match_path(rules, path, is_dir):
    """
    Determine if a certain list of rules match a filesystem entry.

    The rule-checking algorithm also handles rsync-like anchoring of rules
    prefixed with '/'. If the rule is not anchored then it match every
    file whose suffix matches the rule.

    That means that a rule like 'a/b', will match 'a/b' and 'x/a/b' too.
    A rule like '/a/b' will match 'a/b' but not 'x/a/b'.

    If a rule ends with a slash (i.e. 'a/b/') if will be used only if the
    passed path is a directory.

    This function implements the basic wildcards. For more information about
    that, consult the documentation of the "translate_to_regexp" function.

    :param list[str] rules: match
    :param path: the path of the entity to match
    :param is_dir: True if the entity is a directory
    :return bool:
    """
    for rule in rules:
        if rule[-1] == "/":
            if not is_dir:
                continue
            rule = rule[:-1]
        anchored = False
        if rule[0] == "/":
            rule = rule[1:]
            anchored = True
        if _wildcard_match_path(path, rule):
            return True
        if not anchored and _wildcard_match_path(path, "**/" + rule):
            return True
    return False


def _wildcard_match_path(path, pattern):
    """
    Check if the proposed shell pattern match the path passed.

    :param str path:
    :param str pattern:
    :rtype bool: True if it match, False otherwise
    """
    regexp = re.compile(_translate_to_regexp(pattern))
    return regexp.match(path) is not None


def _translate_to_regexp(pattern):
    """
    Translate a shell PATTERN to a regular expression.

    These wildcard characters you to use:

    - "?" to match every character
    - "*" to match zero or more characters, excluding "/"
    - "**" to match zero or more characters, including "/"

    There is no way to quote meta-characters.
    This implementation is based on the one in the Python fnmatch module

    :param str pattern: A string containing wildcards
    """

    i, n = 0, len(pattern)
    res = ""
    while i < n:
        c = pattern[i]
        i = i + 1
        if pattern[i - 1 :].startswith("**"):
            res = res + ".*"
            i = i + 1
        elif c == "*":
            res = res + "[^/]*"
        elif c == "?":
            res = res + "."
        else:
            res = res + re.escape(c)
    return r"(?s)%s\Z" % res


class PathDeletionCommand(with_metaclass(ABCMeta, object)):
    """
    Stand-alone object that will execute delete operation on a self contained path
    """

    @abstractmethod
    def delete(self):
        """
        Will delete the actual path
        """


class LocalLibPathDeletionCommand(PathDeletionCommand):
    def __init__(self, path):
        """
        :param path: str
        """
        self.path = path

    def delete(self):
        shutil.rmtree(self.path, ignore_errors=True)


class UnixCommandPathDeletionCommand(PathDeletionCommand):
    def __init__(self, path, unix_command):
        """

        :param path:
        :param unix_command UnixLocalCommand:
        """
        self.path = path
        self.command = unix_command

    def delete(self):
        self.command.delete_if_exists(self.path)
