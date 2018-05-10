# Copyright (C) 2013-2018 2ndQuadrant Limited
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

from barman.command_wrappers import Command, full_command_quote
from barman.exceptions import FsOperationFailed

_logger = logging.getLogger(__name__)


class UnixLocalCommand(object):
    """
    This class is a wrapper for local calls for file system operations
    """

    def __init__(self, path=None):
        # initialize a shell
        self.internal_cmd = Command(cmd='sh', args=['-c'], path=path)

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

    def create_dir_if_not_exists(self, dir_path):
        """
        This method recursively creates a directory if not exists

        If the path exists and is not a directory raise an exception.

        :param str dir_path: full path for the directory
        """
        _logger.debug('Create directory %s if it does not exists' % dir_path)
        exists = self.exists(dir_path)
        if exists:
            is_dir = self.cmd('test', args=['-d', dir_path])
            if is_dir != 0:
                raise FsOperationFailed(
                    'A file with the same name already exists')
            else:
                return False
        else:
            # Make parent directories if needed
            mkdir_ret = self.cmd('mkdir', args=['-p', dir_path])
            if mkdir_ret == 0:
                return True
            else:
                raise FsOperationFailed('mkdir execution failed')

    def delete_if_exists(self, path):
        """
        This method check for the existence of a path.
        If it exists, then is removed using a rm -fr command,
        and returns True.
        If the command fails an exception is raised.
        If the path does not exists returns False

        :param path the full path for the directory
        """
        _logger.debug('Delete path %s if exists' % path)
        exists = self.exists(path, False)
        if exists:
            rm_ret = self.cmd('rm', args=['-fr', path])
            if rm_ret == 0:
                return True
            else:
                raise FsOperationFailed('rm execution failed')
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
        _logger.debug('Check if directory %s exists' % dir_path)
        exists = self.exists(dir_path)
        if exists:
            is_dir = self.cmd('test', args=['-d', dir_path])
            if is_dir != 0:
                raise FsOperationFailed(
                    'A file with the same name exists, but is not a directory')
            else:
                return True
        else:
            return False

    def check_write_permission(self, dir_path):
        """
            check write permission for barman on a given path.
            Creates a hidden file using touch, then remove the file.
            returns true if the file is written and removed without problems
            raise exception if the creation fails.
            raise exception if the removal fails.

            :param dir_path full dir_path for the directory to check
        """
        _logger.debug('Check if directory %s is writable' % dir_path)
        exists = self.exists(dir_path)
        if exists:
            is_dir = self.cmd('test', args=['-d', dir_path])
            if is_dir == 0:
                can_write = self.cmd(
                    'touch', args=["%s/.barman_write_check" % dir_path])
                if can_write == 0:
                    can_remove = self.cmd(
                        'rm', args=["%s/.barman_write_check" % dir_path])
                    if can_remove == 0:
                        return True
                    else:
                        raise FsOperationFailed('Unable to remove file')
                else:
                    raise FsOperationFailed(
                        'Unable to create write check file')
            else:
                raise FsOperationFailed('%s is not a directory' % dir_path)
        else:
            raise FsOperationFailed('%s does not exists' % dir_path)

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
        _logger.debug('Create symbolic link %s -> %s' % (dst, src))
        exists = self.exists(src)
        if exists:
            exists_dst = self.exists(dst)
            if not exists_dst:
                link = self.cmd('ln', args=['-s', src, dst])
                if link == 0:
                    return True
                else:
                    raise FsOperationFailed('ln command failed')
            else:
                raise FsOperationFailed('ln destination already exists')
        else:
            raise FsOperationFailed('ln source does not exists')

    def get_system_info(self):
        """
            Gather important system information for 'barman diagnose' command
        """
        result = {}
        # self.internal_cmd.out can be None. The str() call will ensure it
        # will be translated to a literal 'None'
        release = ''
        if self.cmd("lsb_release", args=['-a']) == 0:
            release = self.internal_cmd.out.rstrip()
        elif self.exists('/etc/lsb-release'):
            self.cmd('cat', args=['/etc/lsb-release'])
            release = "Ubuntu Linux %s" % self.internal_cmd.out.rstrip()
        elif self.exists('/etc/debian_version'):
            self.cmd('cat', args=['/etc/debian_version'])
            release = "Debian GNU/Linux %s" % self.internal_cmd.out.rstrip()
        elif self.exists('/etc/redhat-release'):
            self.cmd('cat', args=['/etc/redhat-release'])
            release = "RedHat Linux %s" % self.internal_cmd.out.rstrip()
        elif self.cmd('sw_vers') == 0:
            release = self.internal_cmd.out.rstrip()
        result['release'] = release

        self.cmd('uname', args=['-a'])
        result['kernel_ver'] = self.internal_cmd.out.rstrip()
        self.cmd('python', args=['--version', '2>&1'])
        result['python_ver'] = self.internal_cmd.out.rstrip()
        self.cmd('rsync', args=['--version', '2>&1'])
        try:
            result['rsync_ver'] = self.internal_cmd.out.splitlines(
                True)[0].rstrip()
        except IndexError:
            result['rsync_ver'] = ''
        self.cmd('ssh', args=['-V', '2>&1'])
        result['ssh_ver'] = self.internal_cmd.out.rstrip()
        return result

    def get_file_content(self, path):
        """
        Retrieve the content of a file
        If the file doesn't exist or isn't readable, it raises an exception.

        :param str path: full path to the file to read
        """
        _logger.debug('Reading content of file %s' % path)

        result = self.exists(path)
        if not result:
            raise FsOperationFailed('The %s file does not exist' % path)

        result = self.cmd('test', args=['-r', path])
        if result != 0:
            raise FsOperationFailed('The %s file is not readable' % path)

        result = self.cmd('cat', args=[path])
        if result != 0:
            raise FsOperationFailed('Failed to execute "cat \'%s\'"' % path)

        return self.internal_cmd.out

    def exists(self, path, dereference=True):
        """
        Check for the existence of a path.

        :param str path: full path to check
        :param bool dereference: whether dereference symlinks, defaults
            to True
        :return bool: if the file exists or not.
        """
        _logger.debug('check for existence of: %s' % path)
        options = ['-e', path]
        if not dereference:
            options += ['-o', '-L', path]
        result = self.cmd('test', args=options)
        return result == 0

    def ping(self):

        """
        'Ping' the server executing the `true` command.

        :return int: the true cmd result
        """
        _logger.debug('execute the true command')
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
        _logger.debug('list the content of a directory')
        ls_options = []
        if options:
            ls_options += options
        ls_options.append(dir_path)
        self.cmd('ls', args=ls_options)
        return self.internal_cmd.out


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
            raise FsOperationFailed('No ssh command provided')
        self.internal_cmd = Command(ssh_command,
                                    args=ssh_options,
                                    path=path,
                                    shell=True)
        try:
            ret = self.cmd("true")
        except OSError:
            raise FsOperationFailed("Unable to execute %s" % ssh_command)
        if ret != 0:
            raise FsOperationFailed(
                "Connection failed using '%s %s' return code %s" % (
                    ssh_command,
                    ' '.join(ssh_options),
                    ret))
