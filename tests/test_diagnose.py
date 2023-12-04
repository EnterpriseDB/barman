# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2023
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

import json

from mock import Mock, patch

import barman
from barman.cli import diagnose
from barman.utils import redact_passwords
from testing_helpers import build_config_from_dicts


class TestDiagnose(object):
    def setup_method(self, method):
        self.test_config = build_config_from_dicts(
            global_conf=None,
            main_conf={
                "backup_directory": "/some/barman/home/main",
                "archiver": "on",
            },
            test_conf={
                "backup_directory": "/some/barman/home/main",
                "archiver": "on",
            },
        )

        self.test_config_with_pwd = build_config_from_dicts(
            global_conf=None,
            main_conf={
                "backup_directory": "/some/barman/home/main",
                "conninfo": "host=pg01.nowhere user=postgres password=testpassword",
                "archiver": "on",
            },
            test_conf={
                "backup_directory": "/some/barman/home/main",
                "archiver": "on",
            },
        )

        self.test_config_with_models = build_config_from_dicts(
            global_conf=None,
            main_conf={
                "backup_directory": "/some/barman/home/main",
                "archiver": "on",
            },
            with_model=True,
            model_conf={
                "model": "true",
                "cluster": "main",
                "conninfo": "SOME_CONNINFO",
            },
        )

    @patch("barman.cli.output.close_and_exit")
    @patch("barman.diagnose.output.info")
    def test_diagnose_json(self, info_mock_output, close_exit_mock, monkeypatch):
        monkeypatch.setattr(barman, "__config__", self.test_config)
        mock_args = Mock(show_config_source=False)
        diagnose(mock_args)
        info_mock_output.assert_called_once()
        json_output = info_mock_output.call_args[0][0]

        # Assert that the JSON output syntax is correct
        json.loads(json_output)

        mock_args = Mock(show_config_source=True)
        info_mock_output.reset_mock()
        diagnose(mock_args)
        info_mock_output.assert_called_once()
        json_output = info_mock_output.call_args[0][0]

        # Assert that the JSON output syntax is correct
        json.loads(json_output)

    @patch("barman.cli.output.close_and_exit")
    @patch("barman.diagnose.output.info")
    def test_diagnose_json_with_password(
        self, info_mock_output, close_exit_mock, monkeypatch
    ):
        monkeypatch.setattr(barman, "__config__", self.test_config_with_pwd)
        mock_args = Mock(show_config_source=False)
        diagnose(mock_args)
        info_mock_output.assert_called_once()
        json_output = info_mock_output.call_args[0][0]
        json_output = redact_passwords(json_output)

        # Assert that the JSON output syntax is correct
        json.loads(json_output)

    @patch("barman.cli.output.close_and_exit")
    @patch("barman.diagnose.output.info")
    def test_diagnose_rerun(self, info_mock_output, close_exit_mock, monkeypatch):
        monkeypatch.setattr(barman, "__config__", self.test_config)
        mock_args = Mock(show_config_source=False)
        diagnose(mock_args)
        info_mock_output.assert_called_once()
        json_output = info_mock_output.call_args[0][0]

        # Assert that the JSON output syntax is correct
        json.loads(json_output)

        diagnose(mock_args)
        json_output2 = info_mock_output.call_args[0][0]

        # Assert that the JSON output syntax is correct
        json.loads(json_output2)

    @patch("barman.cli.output.close_and_exit")
    @patch("barman.diagnose.output.info")
    def test_diagnose_json_with_models(
        self, info_mock_output, close_exit_mock, monkeypatch
    ):
        monkeypatch.setattr(barman, "__config__", self.test_config_with_models)
        mock_args = Mock(show_config_source=False)
        diagnose(mock_args)
        info_mock_output.assert_called_once()
        json_output = info_mock_output.call_args[0][0]

        # Assert that the JSON output syntax is correct
        json.loads(json_output)

        mock_args = Mock(show_config_source=True)
        info_mock_output.reset_mock()
        diagnose(mock_args)
        info_mock_output.assert_called_once()
        json_output = info_mock_output.call_args[0][0]

        # Assert that the JSON output syntax is correct
        json.loads(json_output)
