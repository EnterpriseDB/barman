# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2021
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

import pytest
from mock import call, patch

from barman.exceptions import FsOperationFailed
from barman.fs import (UnixLocalCommand, _match_path, _translate_to_regexp,
                       _wildcard_match_path, path_allowed)


class TestUnixLocalCommand(object):

    @patch('barman.fs.Command')
    def test_cmd(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()
        result = ulc.cmd('test command')

        assert result == command_instance.return_value
        command_mock.assert_called_once_with(cmd='sh', args=['-c'],
                                             path=None)
        command_instance.assert_called_once_with('test command')

    @patch('barman.fs.Command')
    def test_cmd_path(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand(path='/a:/b')
        result = ulc.cmd('test command')

        assert result == command_instance.return_value
        command_mock.assert_called_once_with(cmd='sh', args=['-c'],
                                             path='/a:/b')
        command_instance.assert_called_once_with('test command')

    @patch('barman.fs.Command')
    def test_get_last_output(self, command_mock):
        command_instance = command_mock.return_value
        command_instance.out = "out"
        command_instance.err = "err"

        ulc = UnixLocalCommand()
        out, err = ulc.get_last_output()
        assert out == 'out'
        assert err == 'err'

    @patch('barman.fs.Command')
    def test_dir_if_not_exists(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()

        # Path exists
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            0,  # is dir
        ]
        result = ulc.create_dir_if_not_exists('test dir')

        assert not result  # dir not created
        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("test '-d' 'test dir'"),
        ]

        # Path does not exist
        command_mock.reset_mock()
        command_instance.side_effect = [
            1,  # exists
            0,  # mkdir
        ]
        result = ulc.create_dir_if_not_exists('test dir')

        assert result  # dir created
        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("mkdir '-p' 'test dir'"),
        ]

        # Path exists and is a file
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            1,  # is dir
        ]
        with pytest.raises(FsOperationFailed):
            ulc.create_dir_if_not_exists('test dir')

        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("test '-d' 'test dir'"),
        ]

        # Path does not exist, but fail creation
        command_mock.reset_mock()
        command_instance.side_effect = [
            1,  # exists
            1,  # mkdir
        ]
        with pytest.raises(FsOperationFailed):
            ulc.create_dir_if_not_exists('test dir')

        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("mkdir '-p' 'test dir'"),
        ]

    @patch('barman.fs.Command')
    def test_delete_if_exists(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()

        # Path does not exist
        command_mock.reset_mock()
        command_instance.side_effect = [
            1,  # exists
        ]
        result = ulc.delete_if_exists('test dir')

        assert not result  # path not deleted
        assert command_instance.mock_calls == [
            call("test '-e' 'test dir' '-o' '-L' 'test dir'"),
        ]

        # Path exists
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            0,  # rm
        ]
        result = ulc.delete_if_exists('test dir')

        assert result  # path deleted
        assert command_instance.mock_calls == [
            call("test '-e' 'test dir' '-o' '-L' 'test dir'"),
            call("rm '-fr' 'test dir'"),
        ]

        # Path exists, but fail deletion
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            1,  # rm
        ]
        with pytest.raises(FsOperationFailed):
            ulc.delete_if_exists('test dir')

        assert command_instance.mock_calls == [
            call("test '-e' 'test dir' '-o' '-L' 'test dir'"),
            call("rm '-fr' 'test dir'"),
        ]

    @patch('barman.fs.Command')
    def test_check_directory_exists(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()

        # Path does not exist
        command_mock.reset_mock()
        command_instance.side_effect = [
            1,  # exists
        ]
        result = ulc.check_directory_exists('test dir')

        assert not result  # path does not exists
        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
        ]

        # Path exists and is a directory
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            0,  # is dir
        ]
        result = ulc.check_directory_exists('test dir')

        assert result  # path exists and is a directory
        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("test '-d' 'test dir'"),
        ]

        # Path exists, but is not a directory
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            1,  # is dir
        ]
        with pytest.raises(FsOperationFailed):
            ulc.check_directory_exists('test dir')

        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("test '-d' 'test dir'"),
        ]

    @patch('barman.fs.Command')
    def test_check_write_permission(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()

        # Path does not exist
        command_mock.reset_mock()
        command_instance.side_effect = [
            1,  # exists
        ]
        with pytest.raises(FsOperationFailed):
            ulc.check_write_permission('test dir')

        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
        ]

        # Path exists but is not a directory
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            1,  # is dir
        ]
        with pytest.raises(FsOperationFailed):
            ulc.check_write_permission('test dir')

        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("test '-d' 'test dir'"),
        ]

        # Path exists, is a directory, but is not writable
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            0,  # is dir
            1,  # can write
        ]
        with pytest.raises(FsOperationFailed):
            ulc.check_write_permission('test dir')

        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("test '-d' 'test dir'"),
            call("touch 'test dir/.barman_write_check'"),
        ]

        # Path exists, is a directory, is writable, but remove failure
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            0,  # is dir
            0,  # can write
            1,  # can remove
        ]
        with pytest.raises(FsOperationFailed):
            ulc.check_write_permission('test dir')

        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("test '-d' 'test dir'"),
            call("touch 'test dir/.barman_write_check'"),
            call("rm 'test dir/.barman_write_check'"),
        ]

        # Path exists, is a directory, is writable, and can remove
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            0,  # is dir
            0,  # can write
            0,  # can remove
        ]
        result = ulc.check_write_permission('test dir')

        assert result
        assert command_instance.mock_calls == [
            call("test '-e' 'test dir'"),
            call("test '-d' 'test dir'"),
            call("touch 'test dir/.barman_write_check'"),
            call("rm 'test dir/.barman_write_check'"),
        ]

    @patch('barman.fs.Command')
    def test_create_symbolic_link(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()

        # Src does not exist
        command_mock.reset_mock()
        command_instance.side_effect = [
            1,  # exists
        ]
        with pytest.raises(FsOperationFailed):
            ulc.create_symbolic_link('test src', 'test dst')

        assert command_instance.mock_calls == [
            call("test '-e' 'test src'"),
        ]

        # Src exists but also dst
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists src
            0,  # exists dst
        ]
        with pytest.raises(FsOperationFailed):
            ulc.create_symbolic_link('test src', 'test dst')

        assert command_instance.mock_calls == [
            call("test '-e' 'test src'"),
            call("test '-e' 'test dst'"),
        ]

        # Path exists, dst does not exist, link creation failed
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists src
            1,  # exists dst
            1,  # link
        ]
        with pytest.raises(FsOperationFailed):
            ulc.create_symbolic_link('test src', 'test dst')

        assert command_instance.mock_calls == [
            call("test '-e' 'test src'"),
            call("test '-e' 'test dst'"),
            call("ln '-s' 'test src' 'test dst'"),
        ]

        # Path exists, dst does not exist, link created
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists src
            1,  # exists dst
            0,  # link
        ]
        result = ulc.create_symbolic_link('test src', 'test dst')

        assert result
        assert command_instance.mock_calls == [
            call("test '-e' 'test src'"),
            call("test '-e' 'test dst'"),
            call("ln '-s' 'test src' 'test dst'"),
        ]

    @patch('barman.fs.Command')
    def test_get_file_content(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()

        # Path does not exist
        command_mock.reset_mock()
        command_instance.side_effect = [
            1,  # exists
        ]
        with pytest.raises(FsOperationFailed):
            ulc.get_file_content('test path')

        assert command_instance.mock_calls == [
            call("test '-e' 'test path'"),
        ]

        # Path exists but is not readable
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            1,  # readable
        ]
        with pytest.raises(FsOperationFailed):
            ulc.get_file_content('test path')

        assert command_instance.mock_calls == [
            call("test '-e' 'test path'"),
            call("test '-r' 'test path'"),
        ]

        # Path exists, is readable, but cat fails
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            0,  # readable
            1,  # cat
        ]
        with pytest.raises(FsOperationFailed):
            ulc.get_file_content('test path')

        assert command_instance.mock_calls == [
            call("test '-e' 'test path'"),
            call("test '-r' 'test path'"),
            call("cat 'test path'"),
        ]

        # Path exists, is readable and cat works
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # exists
            0,  # readable
            0,  # cat
        ]
        command_instance.out = 'content'
        result = ulc.get_file_content('test path')

        assert result == 'content'
        assert command_instance.mock_calls == [
            call("test '-e' 'test path'"),
            call("test '-r' 'test path'"),
            call("cat 'test path'"),
        ]

    @patch('barman.fs.Command')
    def test_ping(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()

        # Ping failed
        command_mock.reset_mock()
        command_instance.side_effect = [
            1,  # true call
        ]
        result = ulc.ping()

        assert result == 1
        assert command_instance.mock_calls == [
            call("true"),
        ]

        # Ping succeeded
        command_mock.reset_mock()
        command_instance.side_effect = [
            0,  # true call
        ]
        result = ulc.ping()

        assert result == 0
        assert command_instance.mock_calls == [
            call("true"),
        ]

    @patch('barman.fs.Command')
    def test_list_dir_content(self, command_mock):
        command_instance = command_mock.return_value

        ulc = UnixLocalCommand()

        # List directory
        command_mock.reset_mock()
        command_instance.out = 'command output'
        result = ulc.list_dir_content('test path')

        assert result == 'command output'
        assert command_instance.mock_calls == [
            call("ls 'test path'"),
        ]

        # List directory with options
        command_mock.reset_mock()
        command_instance.out = 'command output'
        result = ulc.list_dir_content('test path', ['-la'])

        assert result == 'command output'
        assert command_instance.mock_calls == [
            call("ls '-la' 'test path'"),
        ]


class TestFileMatchingRules(object):
    def test_match_dirs_not_anchored(self):
        match = _match_path
        rules = ['one/two/']

        # This match, because two is a directory
        assert match(rules, 'one/two', True)

        # This match, because the rule is not anchored
        assert match(rules, 'zero/one/two', True)

        # This don't match, because two is a file
        assert not match(rules, 'one/two', False)

        # This don't match, even if the rule is not anchored, because
        # two is a file
        assert not match(rules, 'zero/one/two', False)

        # These obviously don't match
        assert not match(rules, 'three/four', False)
        assert not match(rules, 'three/four', True)

    def test_match_dirs_anchored(self):
        match = _match_path
        rules = ['/one/two/']

        # This match, because two is a directory
        assert match(rules, 'one/two', True)

        # This don't match, because the rule is not anchored
        assert not match(rules, 'zero/one/two', True)

        # This don't match, because two is a file
        assert not match(rules, 'one/two', False)

        # This don't match because two is a file
        assert not match(rules, 'zero/one/two', False)

        # These obviously don't match
        assert not match(rules, 'three/four', False)
        assert not match(rules, 'three/four', True)

    def test_match_files_not_anchored(self):
        match = _match_path
        rules = ['one/two']

        # This match, because two is a file
        assert match(rules, 'one/two', False)

        # This match, because the rule is not anchored
        assert match(rules, 'zero/one/two', False)

        # This match, because two is a directory and that is matched also by
        # dirs
        assert match(rules, 'one/two', True)

        # This match, because the rule is not anchored, and
        # two is a directory
        assert match(rules, 'zero/one/two', True)

        # These obviously don't match
        assert not match(rules, 'three/four', False)
        assert not match(rules, 'three/four', True)

    def test_match_files_anchored(self):
        match = _match_path
        rules = ['/one/two']

        # This match, because two is a file
        assert match(rules, 'one/two', False)

        # This don't match, because the rule is not anchored
        assert not match(rules, 'zero/one/two', False)

        # This match, because two is a directory and that is matched also by
        # files
        assert match(rules, 'one/two', True)

        # This don't match because the rule is anchored
        assert not match(rules, 'zero/one/two', True)

        # These obviously don't match
        assert not match(rules, 'three/four', False)
        assert not match(rules, 'three/four', True)

    def test_match_multiple_rules(self):
        match = _match_path
        rules = [
            'one/two',
            'three/four'
        ]

        assert match(rules, 'one/two', True)
        assert match(rules, 'three/four', True)
        assert match(rules, '/one/two', True)
        assert match(rules, '/three/four', True)
        assert not match(rules, 'five/six', True)
        assert not match(rules, 'five/six', True)

        # No rule explicitly match directories, so everything should work
        # for directories too

        assert match(rules, 'one/two', False)
        assert match(rules, 'three/four', False)
        assert match(rules, '/one/two', False)
        assert match(rules, '/three/four', False)
        assert not match(rules, 'five/six', False)
        assert not match(rules, 'five/six', False)

    def test_match_wildcards(self):
        match = _match_path
        rules = [
            'one/two/*.txt',
            'three/four/*.mid',
        ]

        assert match(rules, 'one/two/test.txt', False)
        assert match(rules, 'prefix/one/two/test.txt', False)
        assert not match(rules, 'one/two/test.foo', False)

        assert match(rules, 'three/four/test.mid', False)
        assert not match(rules, 'one/two/three/test.txt', False)


class TestExcludeIncludeRules(object):
    def test_include_rules(self):
        match = path_allowed
        include_rules = ['foo/bar']
        exclude_rules = ['one/two']

        assert match(exclude_rules, include_rules, 'foo/bar', False)

    def test_exclude_rules(self):
        match = path_allowed
        include_rules = ['foo/bar']
        exclude_rules = ['one/two']

        assert not match(exclude_rules, include_rules, 'one/two', False)

    def test_both_include_exclude_rules(self):
        match = path_allowed
        include_rules = ['foo/bar']
        exclude_rules = ['foo/bar']

        assert match(exclude_rules, include_rules, 'foo/bar', False)

    def test_no_matching_rules(self):
        match = path_allowed
        include_rules = ['foo/bar']
        exclude_rules = ['foo/bar']

        assert match(exclude_rules, include_rules, 'one/two', False)

    def test_only_exclude_rules(self):
        match = path_allowed
        include_rules = None
        exclude_rules = ['one/two', 'pg_internal.init']

        assert match(exclude_rules, include_rules, 'foo/bar', False)
        assert not match(exclude_rules, include_rules, 'one/two', False)
        assert not match(exclude_rules, include_rules,
                         'pg_internal.init', False)
        assert not match(exclude_rules, include_rules,
                         'base/13382/pg_internal.init', False)

    def test_only_include_rules(self):
        match = path_allowed
        include_rules = ['foo/bar']
        exclude_rules = None

        assert match(exclude_rules, include_rules, 'one/foo/bar', False)
        assert match(exclude_rules, include_rules, 'bar/foo', False)


class TestWildcardMatch(object):
    def test_exact_match(self):
        assert _wildcard_match_path('text.txt', 'text.txt')
        assert not _wildcard_match_path('text.txt', 'toast.bmp')

    def test_question_mark(self):
        assert _wildcard_match_path('test.txt', 'test.tx?')
        assert not _wildcard_match_path('test.bmp', 'test.tx?')

    def test_asterisk(self):
        assert _wildcard_match_path('test.txt', 'test*')
        assert not _wildcard_match_path('toast.txt', 'test*')

    def test_asterisk_without_slash(self):
        assert _wildcard_match_path('directory/file.txt', 'directory/*.txt')
        assert not _wildcard_match_path('directory/file.txt', '*.txt')

    def test_two_asterisks(self):
        assert _wildcard_match_path('directory/file.txt', '**.txt')
        assert not _wildcard_match_path('directory/file.bmp', '**.txt')


class TestTranslate(object):
    def test_empty_pattern(self):
        assert _translate_to_regexp('') == r'(?s)\Z'

    def test_one_star_pattern(self):
        assert _translate_to_regexp('test*me') == r'(?s)test[^/]*me\Z'

    def test_two_stars_pattern(self):
        assert _translate_to_regexp('test**me') == r'(?s)test.*me\Z'

    def test_question_mark_pattern(self):
        assert _translate_to_regexp('test?me') == r'(?s)test.me\Z'
