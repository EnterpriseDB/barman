# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2025
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

import errno
import io
import os
from abc import ABCMeta, abstractmethod

from barman.exceptions import ArchivalBackupException
from barman.utils import with_metaclass


class AnnotationManager(with_metaclass(ABCMeta)):
    """
    This abstract base class defines the AnnotationManager interface which provides
    methods for read, write and delete of annotations for a given backup.
    """

    @abstractmethod
    def put_annotation(self, backup_id, key, value):
        """Add an annotation"""

    @abstractmethod
    def get_annotation(self, backup_id, key):
        """Get the value of an annotation"""

    @abstractmethod
    def delete_annotation(self, backup_id, key):
        """Delete an annotation"""


class AnnotationManagerFile(AnnotationManager):
    def __init__(self, path, old_path=None):
        """
        Constructor for the file-based annotation manager.
        Should be initialised with the path to the barman base backup directory.

        :param str path: The path where the annotation file should be placed.
        :param str|None old_path: Optional path used to read annotations written
            before Barman 3.13.3.

        .. note:
            Starting from Barman 3.13.3, annotation files were moved out of the base
            backup directory and into a dedicated metadata directory. While this class
            is agnostic about what kind of annotation it stores, backwards compatibility
            is needed for users upgrading from previous versions who may have annotations
            stored in the legacy location. For that reason, the *old_path* parameter
            allows this class to read and migrate existing annotations from the old
            directory as to maintain backwards compatibility.
        """
        self.path = path
        self.old_path = old_path

    def _get_old_annotation_path(self, backup_id, key):
        """
        Builds the annotation path for the specified *backup_id* and annotation *key* for
        annotations created before Barman 3.13.3. Check the note on this class'
        constructor for more context.

        :param str backup_id: The backup ID.
        :param str key: The annotation file name.
        :returns str: The path to the annotation.
        """
        return "%s/%s/annotations/%s" % (self.old_path, backup_id, key)

    def _get_annotation_path(self, backup_id, key):
        """
        Builds the annotation path for the specified backup_id and annotation key.

        :param str backup_id: The backup ID.
        :param str key: The annotation file name.
        :returns str: The path to the annotation.
        """
        return "%s/%s-%s" % (self.path, backup_id, key)

    def _check_and_relocate_old_annotation(self, backup_id, key):
        """
        Check if the annotation exists in the old path, used before Barman 3.13.3,
        and relocate it to the new path as to maintain backwards compatibility.

        :param str backup_id: The backup ID.
        :param str key: The annotation file name.
        """
        if not self.old_path:
            return
        old_path = self._get_old_annotation_path(backup_id, key)
        new_path = self._get_annotation_path(backup_id, key)
        if os.path.exists(old_path):
            os.rename(old_path, new_path)

    def delete_annotation(self, backup_id, key):
        """
        Deletes an annotation from the filesystem for the specified backup_id and
        annotation key.
        """
        self._check_and_relocate_old_annotation(backup_id, key)
        annotation_path = self._get_annotation_path(backup_id, key)
        try:
            os.remove(annotation_path)
        except EnvironmentError as e:
            # For Python 2 compatibility we must check the error code directly
            # If the annotation doesn't exist then the failure to delete it is not an
            # error condition and we should not proceed to remove the annotations
            # directory
            if e.errno == errno.ENOENT:
                return
            else:
                raise
        try:
            os.rmdir(os.path.dirname(annotation_path))
        except EnvironmentError as e:
            # For Python 2 compatibility we must check the error code directly
            # If we couldn't remove the directory because it wasn't empty then we
            # do not consider it an error condition
            if e.errno != errno.ENOTEMPTY:
                raise

    def get_annotation(self, backup_id, key):
        """
        Reads the annotation `key` for the specified backup_id from the filesystem
        and returns the value.
        """
        self._check_and_relocate_old_annotation(backup_id, key)
        annotation_path = self._get_annotation_path(backup_id, key)
        try:
            with open(annotation_path, "r") as annotation_file:
                return annotation_file.read()
        except EnvironmentError as e:
            # For Python 2 compatibility we must check the error code directly
            # If the annotation doesn't exist then return None
            if e.errno != errno.ENOENT:
                raise

    def put_annotation(self, backup_id, key, value):
        """
        Writes the specified value for annotation `key` for the specified backup_id
        to the filesystem.
        """
        annotation_path = self._get_annotation_path(backup_id, key)
        try:
            os.makedirs(os.path.dirname(annotation_path))
        except EnvironmentError as e:
            # For Python 2 compatibility we must check the error code directly
            # If the directory already exists then it is not an error condition
            if e.errno != errno.EEXIST:
                raise
        with open(annotation_path, "w") as annotation_file:
            if value:
                annotation_file.write(value)


class AnnotationManagerCloud(AnnotationManager):
    def __init__(self, cloud_interface, server_name):
        """
        Constructor for the cloud-based annotation manager.
        Should be initialised with the CloudInterface and name of the server which
        was used to create the backups.
        """
        self.cloud_interface = cloud_interface
        self.server_name = server_name
        self.annotation_cache = None

    def _get_base_path(self):
        """
        Returns the base path to the cloud storage, accounting for the fact that
        CloudInterface.path may be None.
        """
        return self.cloud_interface.path and "%s/" % self.cloud_interface.path or ""

    def _get_annotation_path(self, backup_id, key):
        """
        Builds the full key to the annotation in cloud storage for the specified
        backup_id and annotation key.
        """
        return "%s%s/base/%s/annotations/%s" % (
            self._get_base_path(),
            self.server_name,
            backup_id,
            key,
        )

    def _populate_annotation_cache(self):
        """
        Build a cache of which annotations actually exist by walking the bucket.
        This allows us to optimize get_annotation by just checking a (backup_id,key)
        tuple here which is cheaper (in time and money) than going to the cloud
        every time.
        """
        self.annotation_cache = {}
        for object_key in self.cloud_interface.list_bucket(
            os.path.join(self._get_base_path(), self.server_name, "base") + "/",
            delimiter="",
        ):
            key_parts = object_key.split("/")
            if len(key_parts) > 3:
                if key_parts[-2] == "annotations":
                    backup_id = key_parts[-3]
                    annotation_key = key_parts[-1]
                    self.annotation_cache[(backup_id, annotation_key)] = True

    def delete_annotation(self, backup_id, key):
        """
        Deletes an annotation from cloud storage for the specified backup_id and
        annotation key.
        """
        annotation_path = self._get_annotation_path(backup_id, key)
        self.cloud_interface.delete_objects([annotation_path])

    def get_annotation(self, backup_id, key, use_cache=True):
        """
        Reads the annotation `key` for the specified backup_id from cloud storage
        and returns the value.

        The default behaviour is that, when it is first run, it populates a
        cache of the annotations which exist for each backup by walking the
        bucket. Subsequent operations can check that cache and avoid having to
        call remote_open if an annotation is not found in the cache.

        This optimises for the case where annotations are sparse and assumes the
        cost of walking the bucket is less than the cost of the remote_open calls
        which would not return a value.

        In cases where we do not want to walk the bucket up front then the caching
        can be disabled.
        """
        # Optimize for the most common case where there is no annotation
        if use_cache:
            if self.annotation_cache is None:
                self._populate_annotation_cache()
            if (
                self.annotation_cache is not None
                and (backup_id, key) not in self.annotation_cache
            ):
                return None
        # We either know there's an annotation or we haven't used the cache so read
        # it from the cloud
        annotation_path = self._get_annotation_path(backup_id, key)
        annotation_fileobj = self.cloud_interface.remote_open(annotation_path)
        if annotation_fileobj:
            with annotation_fileobj:
                annotation_bytes = annotation_fileobj.readline()
                return annotation_bytes.decode("utf-8")
        else:
            # We intentionally return None if remote_open found nothing
            return None

    def put_annotation(self, backup_id, key, value):
        """
        Writes the specified value for annotation `key` for the specified backup_id
        to cloud storage.
        """
        annotation_path = self._get_annotation_path(backup_id, key)
        self.cloud_interface.upload_fileobj(
            io.BytesIO(value.encode("utf-8")), annotation_path
        )


class KeepManager(with_metaclass(ABCMeta, object)):
    """Abstract base class which defines the KeepManager interface"""

    ANNOTATION_KEY = "keep"

    TARGET_FULL = "full"
    TARGET_STANDALONE = "standalone"

    supported_targets = (TARGET_FULL, TARGET_STANDALONE)

    @abstractmethod
    def should_keep_backup(self, backup_id):
        pass

    @abstractmethod
    def keep_backup(self, backup_id, target):
        pass

    @abstractmethod
    def get_keep_target(self, backup_id):
        pass

    @abstractmethod
    def release_keep(self, backup_id):
        pass


class KeepManagerMixin(KeepManager):
    """
    A Mixin which adds KeepManager functionality to its subclasses.

    Keep management is built on top of annotations and consists of the
    following functionality:
      - Determine whether a given backup is intended to be kept beyond its retention
        period.
      - Determine the intended recovery target for the archival backup.
      - Add and remove the keep annotation.

    The functionality is implemented as a Mixin so that it can be used to add
    keep management to the backup management class in barman (BackupManager)
    as well as its closest analog in barman-cloud (CloudBackupCatalog).
    """

    def __init__(self, *args, **kwargs):
        """
        Base constructor (Mixin pattern).

        kwargs must contain *either*:
          - A barman.server.Server object with the key `server`, *or*:
          - A CloudInterface object and a server name, keys `cloud_interface` and
            `server_name` respectively.

        """
        if "server" in kwargs:
            server = kwargs.pop("server")
            self.annotation_manager = AnnotationManagerFile(
                server.meta_directory, server.config.basebackups_directory
            )
        elif "cloud_interface" in kwargs:
            self.annotation_manager = AnnotationManagerCloud(
                kwargs.pop("cloud_interface"), kwargs.pop("server_name")
            )
        super(KeepManagerMixin, self).__init__(*args, **kwargs)

    def should_keep_backup(self, backup_id):
        """
        Returns True if the specified backup_id for this server has a keep annotation.
        False otherwise.
        """
        return (
            self.annotation_manager.get_annotation(backup_id, type(self).ANNOTATION_KEY)
            is not None
        )

    def keep_backup(self, backup_id, target):
        """
        Add a keep annotation for backup with ID backup_id with the specified
        recovery target.
        """
        if target not in KeepManagerMixin.supported_targets:
            raise ArchivalBackupException("Unsupported recovery target: %s" % target)
        self.annotation_manager.put_annotation(
            backup_id, type(self).ANNOTATION_KEY, target
        )

    def get_keep_target(self, backup_id):
        """Retrieve the intended recovery target"""
        return self.annotation_manager.get_annotation(
            backup_id, type(self).ANNOTATION_KEY
        )

    def release_keep(self, backup_id):
        """Release the keep annotation"""
        self.annotation_manager.delete_annotation(backup_id, type(self).ANNOTATION_KEY)


class KeepManagerMixinCloud(KeepManagerMixin):
    """
    A specialised KeepManager which allows the annotation caching optimization in
    the AnnotationManagerCloud backend to be optionally disabled.
    """

    def should_keep_backup(self, backup_id, use_cache=True):
        """
        Like KeepManagerMixinCloud.should_keep_backup but with the use_cache option.
        """
        return (
            self.annotation_manager.get_annotation(
                backup_id, type(self).ANNOTATION_KEY, use_cache=use_cache
            )
            is not None
        )

    def get_keep_target(self, backup_id, use_cache=True):
        """
        Like KeepManagerMixinCloud.get_keep_target but with the use_cache option.
        """
        return self.annotation_manager.get_annotation(
            backup_id, type(self).ANNOTATION_KEY, use_cache=use_cache
        )
