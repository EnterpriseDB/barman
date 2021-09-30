# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
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

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from openapi_server.models.base_model_ import Model
from openapi_server import util


class DiagnoseOutput(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, _global=None, servers=None):  # noqa: E501
        """DiagnoseOutput - a model defined in OpenAPI

        :param _global: The _global of this DiagnoseOutput.  # noqa: E501
        :type _global: str
        :param servers: The servers of this DiagnoseOutput.  # noqa: E501
        :type servers: str
        """
        self.openapi_types = {"_global": str, "servers": str}

        self.attribute_map = {"_global": "global", "servers": "servers"}

        self.__global = _global
        self._servers = servers

    @classmethod
    def from_dict(cls, dikt) -> "DiagnoseOutput":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The DiagnoseOutput of this DiagnoseOutput.  # noqa: E501
        :rtype: DiagnoseOutput
        """
        return util.deserialize_model(dikt, cls)

    @property
    def _global(self):
        """Gets the _global of this DiagnoseOutput.


        :return: The _global of this DiagnoseOutput.
        :rtype: str
        """
        return self.__global

    @_global.setter
    def _global(self, _global):
        """Sets the _global of this DiagnoseOutput.


        :param _global: The _global of this DiagnoseOutput.
        :type _global: str
        """
        if _global is None:
            raise ValueError(
                "Invalid value for `_global`, must not be `None`"
            )  # noqa: E501

        self.__global = _global

    @property
    def servers(self):
        """Gets the servers of this DiagnoseOutput.


        :return: The servers of this DiagnoseOutput.
        :rtype: str
        """
        return self._servers

    @servers.setter
    def servers(self, servers):
        """Sets the servers of this DiagnoseOutput.


        :param servers: The servers of this DiagnoseOutput.
        :type servers: str
        """
        if servers is None:
            raise ValueError(
                "Invalid value for `servers`, must not be `None`"
            )  # noqa: E501

        self._servers = servers
