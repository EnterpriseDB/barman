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

import io
import mock
import os

from barman.annotations import AnnotationManagerCloud, AnnotationManagerFile

test_backup_id = "20210723T095432"


class TestAnnotationManagerFile(object):
    """
    Tests the functionality of the file-based annotation manager.
    Because we are using a filesystem backend we use a temporary directory and
    verify the actual state.
    """

    def _create_annotation_on_filesystem(self, base_dir, backup_id, key, value):
        """
        Helper which creates an annotation in the correct place on the filesystem.
        """
        with open(
            "%s/%s/annotations/%s" % (base_dir, backup_id, key),
            "w",
        ) as annotation:
            annotation.write(value)

    def _get_annotation_from_filesystem(self, base_dir, backup_id, key):
        """
        Helper which retrieves the value of an annotation from the filesystem.
        """
        with open(
            "%s/%s/annotations/%s" % (base_dir, backup_id, key), "r"
        ) as annotation:
            return annotation.read()

    def test_get_annotation_missing_backup(self, tmpdir):
        """Getting an annotation for a backup which doesn't exist returns None"""
        base_backup_dir = tmpdir.mkdir("base")
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        assert (
            annotation_manager.get_annotation(test_backup_id, "test_annotation") is None
        )

    def test_get_annotation_missing_annotation(self, tmpdir):
        """Getting an annotation which doesn't exist returns None"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s/annotations" % (base_backup_dir, test_backup_id))
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        assert (
            annotation_manager.get_annotation(test_backup_id, "test_annotation") is None
        )

    def test_get_annotation(self, tmpdir):
        """Tests getting the value of a single annotation"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s/annotations" % (base_backup_dir, test_backup_id))
        self._create_annotation_on_filesystem(
            base_backup_dir, test_backup_id, "test_annotation", "annotation_value"
        )
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        assert (
            annotation_manager.get_annotation(test_backup_id, "test_annotation")
            == "annotation_value"
        )

    def test_get_one_of_many_annotation(self, tmpdir):
        """Tests getting the value of one of multiple annotations"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s/annotations" % (base_backup_dir, test_backup_id))
        self._create_annotation_on_filesystem(
            base_backup_dir, test_backup_id, "test_annotation", "annotation_value"
        )
        self._create_annotation_on_filesystem(
            base_backup_dir, test_backup_id, "test_annotation_2", "annotation_value_2"
        )
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        assert (
            annotation_manager.get_annotation(test_backup_id, "test_annotation")
            == "annotation_value"
        )

    def test_put_annotation(self, tmpdir):
        """Tests a single annotation is stored"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s" % (base_backup_dir, test_backup_id))
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation", "annotation_value"
        )
        assert (
            self._get_annotation_from_filesystem(
                base_backup_dir, test_backup_id, "test_annotation"
            )
            == "annotation_value"
        )

    def test_put_annotation_is_idempotent(self, tmpdir):
        """Tests a single annotation can be added multiple times with the same result"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s" % (base_backup_dir, test_backup_id))
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation", "annotation_value"
        )
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation", "annotation_value"
        )
        assert (
            self._get_annotation_from_filesystem(
                base_backup_dir, test_backup_id, "test_annotation"
            )
            == "annotation_value"
        )

    def test_put_annotation_overwrite(self, tmpdir):
        """Tests a single annotation can be overwritten"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s" % (base_backup_dir, test_backup_id))
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation", "annotation_value"
        )
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation", "annotation_value_2"
        )
        assert (
            self._get_annotation_from_filesystem(
                base_backup_dir, test_backup_id, "test_annotation"
            )
            == "annotation_value_2"
        )

    def test_put_multiple_annotations(self, tmpdir):
        """Tests multiple annotations can be written"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s" % (base_backup_dir, test_backup_id))
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation", "annotation_value"
        )
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation_2", "annotation_value_2"
        )
        assert (
            self._get_annotation_from_filesystem(
                base_backup_dir, test_backup_id, "test_annotation"
            )
            == "annotation_value"
        )
        assert (
            self._get_annotation_from_filesystem(
                base_backup_dir, test_backup_id, "test_annotation_2"
            )
            == "annotation_value_2"
        )

    def test_put_annotation_for_missing_backup(self, tmpdir):
        """Tests we can annotate a backup which doesn't exist"""
        base_backup_dir = tmpdir.mkdir("base")
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation", "annotation_value"
        )
        assert (
            self._get_annotation_from_filesystem(
                base_backup_dir, test_backup_id, "test_annotation"
            )
            == "annotation_value"
        )

    def test_delete_annotation(self, tmpdir):
        """Tests we delete an annotation successfully"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s/annotations" % (base_backup_dir, test_backup_id))
        self._create_annotation_on_filesystem(
            base_backup_dir, test_backup_id, "test_annotation", "annotation_value"
        )
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        annotation_manager.delete_annotation(test_backup_id, "test_annotation")
        assert not os.path.isfile(
            "%s/%s/annotations/test_annotation" % (base_backup_dir, test_backup_id)
        )
        assert not os.path.isfile(
            "%s/%s/annotations" % (base_backup_dir, test_backup_id)
        )
        assert os.path.isdir("%s/%s" % (base_backup_dir, test_backup_id))

    def test_delete_one_of_many_annotations(self, tmpdir):
        """Tests we delete the correct annotation successfully"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s/annotations" % (base_backup_dir, test_backup_id))
        self._create_annotation_on_filesystem(
            base_backup_dir, test_backup_id, "test_annotation", "annotation_value"
        )
        self._create_annotation_on_filesystem(
            base_backup_dir, test_backup_id, "test_annotation_2", "annotation_value_2"
        )
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        annotation_manager.delete_annotation(test_backup_id, "test_annotation")
        assert not os.path.isfile(
            "%s/%s/annotations/test_annotation" % (base_backup_dir, test_backup_id)
        )
        assert (
            self._get_annotation_from_filesystem(
                base_backup_dir, test_backup_id, "test_annotation_2"
            )
            == "annotation_value_2"
        )

    def test_delete_is_idempotent(self, tmpdir):
        """Tests a single annotation can be deleted multiple times with the same result"""
        base_backup_dir = tmpdir.mkdir("base")
        os.makedirs("%s/%s/annotations" % (base_backup_dir, test_backup_id))
        self._create_annotation_on_filesystem(
            base_backup_dir, test_backup_id, "test_annotation", "annotation_value"
        )
        annotation_manager = AnnotationManagerFile(base_backup_dir)
        annotation_manager.delete_annotation(test_backup_id, "test_annotation")
        annotation_manager.delete_annotation(test_backup_id, "test_annotation")


class TestAnnotationManagerCloud(object):
    """
    Tests the functionality of the cloud-based annotation manager.
    Rather than verify the state in the cloud provider we take the approach used
    elsewhere for barman-cloud and verify the calls to CloudInterface instead.
    """

    @mock.patch("barman.cloud.CloudInterface")
    def test__get_annotation_path(self, mock_cloud_interface):
        """Tests _get_annotation_path when CloudInterface has no .path"""
        mock_cloud_interface.path = None
        annotation_manager = AnnotationManagerCloud(mock_cloud_interface, "test_server")
        assert (
            annotation_manager._get_annotation_path(test_backup_id, "test_annotation")
            == "test_server/base/%s/annotations/test_annotation" % test_backup_id
        )

    @mock.patch("barman.cloud.CloudInterface")
    def test__get_annotation_path_with_cloud_interface_path(self, mock_cloud_interface):
        """Tests _get_annotation_path when CloudInterface has a .path"""
        mock_cloud_interface.path = "a/path/in/the/cloud"
        annotation_manager = AnnotationManagerCloud(mock_cloud_interface, "test_server")
        assert (
            annotation_manager._get_annotation_path(test_backup_id, "test_annotation")
            == "a/path/in/the/cloud/test_server/base/%s/annotations/test_annotation"
            % test_backup_id
        )

    @mock.patch("barman.cloud.CloudInterface")
    def test_get_missing_annotation(self, mock_cloud_interface):
        """
        Getting a missing annotation returns None.

        Because cloud storage only cares about keys and has no hard notion of
        directories we do not need a separate test case for missing backups.
        """
        annotation_manager = AnnotationManagerCloud(mock_cloud_interface, "test_server")
        mock_cloud_interface.remote_open.return_value = None
        assert (
            annotation_manager.get_annotation(test_backup_id, "test_annotation") is None
        )

    @mock.patch("barman.cloud.CloudInterface")
    def test_get_missing_annotation_cache_optimisation(self, mock_cloud_interface):
        """
        Tests that we avoid remote_open calls for missing annotations.
        """
        mock_cloud_interface.path = None
        annotation_manager = AnnotationManagerCloud(mock_cloud_interface, "test_server")
        mock_cloud_interface.list_bucket.return_value = iter(
            [
                "test_server/base/%s/annotations/test_annotation" % test_backup_id,
            ]
        )
        # Deliberately try to fetch an annotation which isn't there
        assert (
            annotation_manager.get_annotation(test_backup_id, "test_annotation_2")
            is None
        )
        # The AnnotationManager did not have to open the annotation to determine it
        # was missing
        mock_cloud_interface.remote_open.assert_not_called()

    @mock.patch("barman.cloud.CloudInterface")
    def test_get_missing_annotation_bypass_cache_optimisation(
        self, mock_cloud_interface
    ):
        """
        Tests that we bypass the cache optimisation for missing annotations by default.
        """
        mock_cloud_interface.path = None
        annotation_manager = AnnotationManagerCloud(mock_cloud_interface, "test_server")
        # Deliberately try to fetch an annotation which isn't there without using the
        # cache optimization
        annotation_manager.get_annotation(
            test_backup_id, "test_annotation_2", use_cache=False
        )
        mock_cloud_interface.remote_open.assert_called_once_with(
            "test_server/base/%s/annotations/test_annotation_2" % test_backup_id
        )
        # The AnnotationManager did not have to list the bucket to populate the cache
        mock_cloud_interface.list_bucket.assert_not_called()

    @mock.patch("barman.cloud.CloudInterface")
    def test_get_annotation(self, mock_cloud_interface):
        """
        Tests getting the value of a single annotation.

        Because cloud storage only cares about keys, and because we mock the cloud
        interface to return a specific response for a specific key, we do not have
        separate tests here for getting one of multiple annotations.
        """
        mock_cloud_interface.path = None
        annotation_manager = AnnotationManagerCloud(mock_cloud_interface, "test_server")
        mock_cloud_interface.list_bucket.return_value = iter(
            [
                "test_server/base/%s/annotations/test_annotation" % test_backup_id,
            ]
        )
        mock_cloud_interface.remote_open.return_value = io.BytesIO(
            "annotation_value".encode("utf-8")
        )
        assert (
            annotation_manager.get_annotation(test_backup_id, "test_annotation")
            == "annotation_value"
        )
        mock_cloud_interface.remote_open.assert_called_once_with(
            "test_server/base/%s/annotations/test_annotation" % (test_backup_id)
        )

    @mock.patch("barman.cloud.CloudInterface")
    def test_put_annotation(self, mock_cloud_interface):
        """
        Tests a single annotation is stored.

        We do not test beyond ensuring we pass the expected arguments on to
        CloudInterface because the behaviour of upload_fileobj with respect to
        idempotency and overwriting values is an implementation concern at the
        CloudInterface level.
        """
        mock_cloud_interface.path = None
        annotation_manager = AnnotationManagerCloud(mock_cloud_interface, "test_server")
        annotation_manager.put_annotation(
            test_backup_id, "test_annotation", "annotation_value"
        )
        mock_cloud_interface.upload_fileobj.assert_called_once()
        upload_value = mock_cloud_interface.upload_fileobj.call_args_list[0][0][0]
        assert upload_value.read() == b"annotation_value"
        upload_key = mock_cloud_interface.upload_fileobj.call_args_list[0][0][1]
        assert upload_key == "test_server/base/%s/annotations/test_annotation" % (
            test_backup_id
        )

    @mock.patch("barman.cloud.CloudInterface")
    def test_delete_annotation(self, mock_cloud_interface):
        """
        Tests we delete an annotation successfully.

        As with test_put_annotation, we only test that the expected arguments are
        passed to the CloudInterface and do not verify implementation details at the
        CloudInterface level.
        """
        mock_cloud_interface.path = None
        annotation_manager = AnnotationManagerCloud(mock_cloud_interface, "test_server")
        annotation_manager.delete_annotation(test_backup_id, "test_annotation")
        mock_cloud_interface.delete_objects.assert_called_once_with(
            ["test_server/base/%s/annotations/test_annotation" % test_backup_id]
        )
