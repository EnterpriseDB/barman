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

import ast
import collections
import inspect
import logging
import os
import re

import dateutil.parser
import dateutil.tz

from barman import xlog
from barman.cloud_providers import snapshots_info_from_dict
from barman.exceptions import BackupInfoBadInitialisation
from barman.utils import fsync_dir

# Named tuple representing a Tablespace with 'name' 'oid' and 'location'
# as property.
Tablespace = collections.namedtuple("Tablespace", "name oid location")

# Named tuple representing a file 'path' with an associated 'file_type'
TypedFile = collections.namedtuple("ConfFile", "file_type path")


def output_snapshots_info(snapshots_info):
    return null_repr(snapshots_info.to_dict())


def load_snapshots_info(string):
    obj = ast.literal_eval(string)
    return snapshots_info_from_dict(obj)


_logger = logging.getLogger(__name__)


def output_tablespace_list(tablespaces):
    """
    Return the literal representation of tablespaces as a Python string

    :param tablespaces tablespaces: list of Tablespaces objects
    :return str: Literal representation of tablespaces
    """
    if tablespaces:
        return repr([tuple(item) for item in tablespaces])
    else:
        return None


def load_tablespace_list(string):
    """
    Load the tablespaces as a Python list of namedtuple
    Uses ast to evaluate information about tablespaces.
    The returned list is used to create a list of namedtuple

    :param str string:
    :return list: list of namedtuple representing all the tablespaces
    """
    obj = ast.literal_eval(string)
    if obj:
        return [Tablespace._make(item) for item in obj]
    else:
        return None


def null_repr(obj):
    """
    Return the literal representation of an object

    :param object obj: object to represent
    :return str|None: Literal representation of an object or None
    """
    return repr(obj) if obj else None


def load_datetime_tz(time_str):
    """
    Load datetime and ensure the result is timezone-aware.

    If the parsed timestamp is naive, transform it into a timezone-aware one
    using the local timezone.

    :param str time_str: string representing a timestamp
    :return datetime: the parsed timezone-aware datetime
    """
    # dateutil parser returns naive or tz-aware string depending on the format
    # of the input string
    timestamp = dateutil.parser.parse(time_str)
    # if the parsed timestamp is naive, forces it to local timezone
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=dateutil.tz.tzlocal())
    return timestamp


def dump_backup_ids(ids):
    """
    Dump a list of backup IDs to disk as a string.

    :param list[str]|None ids: list of backup IDs, if any
    :return str|None: the dumped string.
    """
    if ids:
        return ",".join(ids)
    else:
        return None


def load_backup_ids(string):
    """
    Load a list of backup IDs from disk as a :class:`list`.

    :param string: the string to be loaded as a list.
    :return list[str]|None: the list of backup IDs, if any.
    """
    if string:
        return string.split(",")
    else:
        return None


class Field(object):
    def __init__(self, name, dump=None, load=None, default=None, doc=None):
        """
        Field descriptor to be used with a FieldListFile subclass.

        The resulting field is like a normal attribute with
        two optional associated function: to_str and from_str

        The Field descriptor can also be used as a decorator

            class C(FieldListFile):
                x = Field('x')
                @x.dump
                def x(val): return '0x%x' % val
                @x.load
                def x(val): return int(val, 16)

        :param str name: the name of this attribute
        :param callable dump: function used to dump the content to a disk
        :param callable load: function used to reload the content from disk
        :param default: default value for the field
        :param str doc: docstring of the filed
        """
        self.name = name
        self.to_str = dump
        self.from_str = load
        self.default = default
        self.__doc__ = doc

    # noinspection PyUnusedLocal
    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if not hasattr(obj, "_fields"):
            obj._fields = {}
        return obj._fields.setdefault(self.name, self.default)

    def __set__(self, obj, value):
        if not hasattr(obj, "_fields"):
            obj._fields = {}
        obj._fields[self.name] = value

    def __delete__(self, obj):
        raise AttributeError("can't delete attribute")

    def dump(self, to_str):
        return type(self)(self.name, to_str, self.from_str, self.__doc__)

    def load(self, from_str):
        return type(self)(self.name, self.to_str, from_str, self.__doc__)


class FieldListFile(object):
    __slots__ = ("_fields", "filename")

    # A list of fields which should be hidden if they are not set.
    # Such fields will not be written to backup.info files or included in the
    # backup.info items unles they are set to a non-None value.
    # Any fields listed here should be removed from the list at the next major
    # version increase.
    _hide_if_null = ()

    def __init__(self, **kwargs):
        """
        Represent a predefined set of keys with the associated value.

        The constructor build the object assigning every keyword argument to
        the corresponding attribute. If a provided keyword argument doesn't
        has a corresponding attribute an AttributeError exception is raised.

        The values provided to the constructor must be of the appropriate
        type for the corresponding attribute.
        The constructor will not attempt any validation or conversion on them.

        This class is meant to be an abstract base class.

        :raises: AttributeError
        """
        self._fields = {}
        self.filename = None
        for name in kwargs:
            field = getattr(type(self), name, None)
            if isinstance(field, Field):
                setattr(self, name, kwargs[name])
            else:
                raise AttributeError("unknown attribute %s" % name)

    @classmethod
    def from_meta_file(cls, filename):
        """
        Factory method that read the specified file and build
        an object with its content.

        :param str filename: the file to read
        """
        o = cls()
        o.load(filename)
        return o

    def save(self, filename=None, file_object=None):
        """
        Serialize the object to the specified file or file object

        If a file_object is specified it will be used.

        If the filename is not specified it uses the one memorized in the
        filename attribute. If neither the filename attribute and parameter are
        set a ValueError exception is raised.

        :param str filename: path of the file to write
        :param file file_object: a file like object to write in
        :param str filename: the file to write
        :raises: ValueError
        """
        if file_object:
            info = file_object
        else:
            filename = filename or self.filename
            if filename:
                info = open(filename + ".tmp", "wb")
            else:
                info = None

        if not info:
            raise ValueError(
                "either a valid filename or a file_object must be specified"
            )

        try:
            for name, field in sorted(inspect.getmembers(type(self))):
                value = getattr(self, name, None)
                if value is None and name in self._hide_if_null:
                    continue
                if isinstance(field, Field):
                    if callable(field.to_str):
                        value = field.to_str(value)
                    info.write(("%s=%s\n" % (name, value)).encode("UTF-8"))
        finally:
            if not file_object:
                info.close()

        if not file_object:
            os.rename(filename + ".tmp", filename)
            fsync_dir(os.path.normpath(os.path.dirname(filename)))

    def load(self, filename=None, file_object=None):
        """
        Replaces the current object content with the one deserialized from
        the provided file.

        This method set the filename attribute.

        A ValueError exception is raised if the provided file contains any
        invalid line.

        :param str filename: path of the file to read
        :param file file_object: a file like object to read from
        :param str filename: the file to read
        :raises: ValueError
        """

        if file_object:
            info = file_object
        elif filename:
            info = open(filename, "rb")
        else:
            raise ValueError("either filename or file_object must be specified")

        # detect the filename if a file_object is passed
        if not filename and file_object:
            if hasattr(file_object, "name"):
                filename = file_object.name

        # canonicalize filename
        if filename:
            self.filename = os.path.abspath(filename)
        else:
            self.filename = None
            filename = "<UNKNOWN>"  # This is only for error reporting

        with info:
            for line in info:
                line = line.decode("UTF-8")
                # skip spaces and comments
                if line.isspace() or line.rstrip().startswith("#"):
                    continue

                # parse the line of form "key = value"
                try:
                    name, value = [x.strip() for x in line.split("=", 1)]
                except ValueError:
                    raise ValueError(
                        "invalid line %s in file %s" % (line.strip(), filename)
                    )

                # use the from_str function to parse the value
                field = getattr(type(self), name, None)
                if value == "None":
                    value = None
                elif isinstance(field, Field) and callable(field.from_str):
                    value = field.from_str(value)
                setattr(self, name, value)

    def items(self):
        """
        Return a generator returning a list of (key, value) pairs.

        If a filed has a dump function defined, it will be used.
        """
        for name, field in sorted(inspect.getmembers(type(self))):
            value = getattr(self, name, None)
            if value is None and name in self._hide_if_null:
                continue
            if isinstance(field, Field):
                if callable(field.to_str):
                    value = field.to_str(value)
                yield (name, value)

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join(["%s=%r" % x for x in self.items()]),
        )


class WalFileInfo(FieldListFile):
    """
    Metadata of a WAL file.
    """

    __slots__ = ("orig_filename",)

    name = Field("name", doc="base name of WAL file")
    size = Field("size", load=int, doc="WAL file size after compression")
    time = Field(
        "time", load=float, doc="WAL file modification time (seconds since epoch)"
    )
    compression = Field("compression", doc="compression type")
    encryption = Field("encryption", doc="encryption type")

    @classmethod
    def from_file(
        cls,
        filename,
        compression_manager=None,
        unidentified_compression=None,
        encryption_manager=None,
        **kwargs,
    ):
        """
        Factory method to generate a WalFileInfo from a WAL file.

        Every keyword argument will override any attribute from the provided
        file. If a keyword argument doesn't has a corresponding attribute
        an AttributeError exception is raised.

        :param str filename: the file to inspect
        :param Compressionmanager compression_manager: a compression manager
            which will be used to identify the compression
        :param EncryptionManager encryption_manager: an encryption manager which
            will be used to identify the encryption
        :param str unidentified_compression: the compression to set if
            the current schema is not identifiable
        """
        stat = os.stat(filename)
        kwargs.setdefault("name", os.path.basename(filename))
        kwargs.setdefault("size", stat.st_size)
        kwargs.setdefault("time", stat.st_mtime)
        kwargs.setdefault(
            "encryption", encryption_manager.identify_encryption(filename)
        )
        if "compression" not in kwargs:
            # If the file is encrypted we are not able to identify any compression
            if kwargs["encryption"] is not None:
                kwargs["compression"] = None
            else:
                kwargs["compression"] = (
                    compression_manager.identify_compression(filename)
                    or unidentified_compression
                )
        obj = cls(**kwargs)
        obj.filename = "%s.meta" % filename
        obj.orig_filename = filename
        return obj

    def to_xlogdb_line(self):
        """
        Format the content of this object as a xlogdb line.
        """
        return "%s\t%s\t%s\t%s\t%s\n" % (
            self.name,
            self.size,
            self.time,
            self.compression,
            self.encryption,
        )

    @classmethod
    def from_xlogdb_line(cls, line):
        """
        Parse a line from xlog catalogue

        :param str line: a line in the wal database to parse
        :rtype: WalFileInfo
        """
        parts = line.split()
        # Checks length to keep compatibility with old xlog files where
        # compression and/or encryption did not exist yet
        if len(parts) == 3:
            name, size, time, compression, encryption = parts + [None, None]
        elif len(parts) == 4:
            name, size, time, compression, encryption = parts + [None]
        elif len(parts) == 5:
            name, size, time, compression, encryption = parts
        else:
            raise ValueError("cannot parse line: %r" % (line,))
        # The to_xlogdb_line method writes None values as literal 'None'
        if compression == "None":
            compression = None
        if encryption == "None":
            encryption = None
        size = int(size)
        time = float(time)
        return cls(
            name=name,
            size=size,
            time=time,
            compression=compression,
            encryption=encryption,
        )

    def to_json(self):
        """
        Return an equivalent dictionary that can be encoded in json
        """
        return dict(self.items())

    def relpath(self):
        """
        Returns the WAL file path relative to the server's wals_directory
        """
        return os.path.join(xlog.hash_dir(self.name), self.name)

    def fullpath(self, server):
        """
        Returns the WAL file full path

        :param barman.server.Server server: the server that owns the wal file
        """
        return os.path.join(server.config.wals_directory, self.relpath())


class BackupInfo(FieldListFile):
    #: Conversion to string
    EMPTY = "EMPTY"
    STARTED = "STARTED"
    FAILED = "FAILED"
    WAITING_FOR_WALS = "WAITING_FOR_WALS"
    DONE = "DONE"
    SYNCING = "SYNCING"
    STATUS_COPY_DONE = (WAITING_FOR_WALS, DONE)
    STATUS_ALL = (EMPTY, STARTED, WAITING_FOR_WALS, DONE, SYNCING, FAILED)
    STATUS_NOT_EMPTY = (STARTED, WAITING_FOR_WALS, DONE, SYNCING, FAILED)
    STATUS_ARCHIVING = (STARTED, WAITING_FOR_WALS, DONE, SYNCING)

    #: Status according to retention policies
    OBSOLETE = "OBSOLETE"
    VALID = "VALID"
    POTENTIALLY_OBSOLETE = "OBSOLETE*"
    NONE = "-"
    KEEP_FULL = "KEEP:FULL"
    KEEP_STANDALONE = "KEEP:STANDALONE"
    RETENTION_STATUS = (
        OBSOLETE,
        VALID,
        POTENTIALLY_OBSOLETE,
        KEEP_FULL,
        KEEP_STANDALONE,
        NONE,
    )

    # Backup types according to `backup_method``
    FULL = "full"
    INCREMENTAL = "incremental"
    RSYNC = "rsync"
    SNAPSHOT = "snapshot"
    NOT_INCREMENTAL = (FULL, RSYNC, SNAPSHOT)
    BACKUP_TYPE_ALL = (FULL, INCREMENTAL, RSYNC, SNAPSHOT)

    version = Field("version", load=int)
    pgdata = Field("pgdata")
    # Parse the tablespaces as a literal Python list of namedtuple
    # Output the tablespaces as a literal Python list of tuple
    tablespaces = Field(
        "tablespaces", load=load_tablespace_list, dump=output_tablespace_list
    )
    # Timeline is an integer
    timeline = Field("timeline", load=int)
    begin_time = Field("begin_time", load=load_datetime_tz)
    begin_xlog = Field("begin_xlog")
    begin_wal = Field("begin_wal")
    begin_offset = Field("begin_offset", load=int)
    size = Field("size", load=int)
    deduplicated_size = Field("deduplicated_size", load=int)
    end_time = Field("end_time", load=load_datetime_tz)
    end_xlog = Field("end_xlog")
    end_wal = Field("end_wal")
    end_offset = Field("end_offset", load=int)
    status = Field("status", default=EMPTY)
    server_name = Field("server_name")
    error = Field("error")
    mode = Field("mode")
    config_file = Field("config_file")
    hba_file = Field("hba_file")
    ident_file = Field("ident_file")
    included_files = Field("included_files", load=ast.literal_eval, dump=null_repr)
    backup_label = Field("backup_label", load=ast.literal_eval, dump=null_repr)
    copy_stats = Field("copy_stats", load=ast.literal_eval, dump=null_repr)
    xlog_segment_size = Field(
        "xlog_segment_size", load=int, default=xlog.DEFAULT_XLOG_SEG_SIZE
    )
    systemid = Field("systemid")
    compression = Field("compression")
    backup_name = Field("backup_name")
    snapshots_info = Field(
        "snapshots_info", load=load_snapshots_info, dump=output_snapshots_info
    )
    data_checksums = Field("data_checksums")
    summarize_wal = Field("summarize_wal")
    parent_backup_id = Field("parent_backup_id")
    children_backup_ids = Field(
        "children_backup_ids",
        dump=dump_backup_ids,
        load=load_backup_ids,
    )

    cluster_size = Field("cluster_size", load=int)

    encryption = Field("encryption")

    __slots__ = "backup_id", "backup_version"

    _hide_if_null = ("backup_name", "snapshots_info")

    def __init__(self, backup_id, **kwargs):
        """
        Stores meta information about a single backup

        :param str,None backup_id:
        """
        self.backup_version = 2
        self.backup_id = backup_id
        super(BackupInfo, self).__init__(**kwargs)

    def get_required_wal_segments(self):
        """
        Get the list of required WAL segments for the current backup
        """
        return xlog.generate_segment_names(
            self.begin_wal, self.end_wal, self.version, self.xlog_segment_size
        )

    def get_external_config_files(self):
        """
        Identify all the configuration files that reside outside the PGDATA.

        Returns a list of TypedFile objects.

        :rtype: list[TypedFile]
        """

        config_files = []
        for file_type in ("config_file", "hba_file", "ident_file"):
            config_file = getattr(self, file_type, None)
            if config_file:
                # Consider only those that reside outside of the original
                # PGDATA directory
                if config_file.startswith(self.pgdata):
                    _logger.debug(
                        "Config file '%s' already in PGDATA",
                        config_file[len(self.pgdata) + 1 :],
                    )
                    continue
                config_files.append(TypedFile(file_type, config_file))
        # Check for any include directives in PostgreSQL configuration
        # Currently, include directives are not supported for files that
        # reside outside PGDATA. These files must be manually backed up.
        # Barman will emit a warning and list those files
        if self.included_files:
            for included_file in self.included_files:
                if not included_file.startswith(self.pgdata):
                    config_files.append(TypedFile("include", included_file))
        return config_files

    def set_attribute(self, key, value):
        """
        Set a value for a given key
        """
        setattr(self, key, value)

    @property
    def is_incremental(self):
        """
        Only checks if the backup_info is an incremental backup

        .. note::
            This property only makes sense in the context of local backups stored in the
            Barman server. However, this property is used for retention policies
            processing, code which is shared among local and cloud backups. As this
            property always returns ``False`` for cloud backups, it can safely be reused
            in their code paths as well, though.

        :return bool: ``True`` if this backup has a parent, ``False`` otherwise.
        """
        return self.parent_backup_id is not None

    @property
    def has_children(self):
        """
        Only checks if the backup_info has children

        .. note::
            This property only makes sense in the context of local backups stored in the
            Barman server. However, this property is used for retention policies
            processing, code which is shared among local and cloud backups. As this
            property always returns ``False`` for cloud backups, it can safely be reused
            in their code paths as well, though.

        :return bool: ``True`` if this backup has at least one child, ``False`` otherwise.
        """
        return self.children_backup_ids is not None

    @property
    def backup_type(self):
        """
        Returns a string with the backup type label.

        .. note::
            Even though this property is available in this base class, it is not
            expected to be used in the context of cloud backups.

        The backup type can be one of the following:
        - ``snapshot``: If the backup mode starts with ``snapshot``.
        - ``rsync``: If the backup mode starts with ``rsync``.
        - ``incremental``: If the mode is ``postgres`` and the backup is incremental.
        - ``full``: If the mode is ``postgres`` and the backup is not incremental.

        :return str: The backup type label.
        """
        if self.mode.startswith("snapshot"):
            return "snapshot"
        elif self.mode.startswith("rsync"):
            return "rsync"
        return "incremental" if self.is_incremental else "full"

    @property
    def deduplication_ratio(self):
        """
        Returns a value between and including ``0`` and ``1`` related to the estimate
        deduplication ratio of the backup.

        .. note::
            For ``rsync`` backups, the :attr:`size` of the backup, which is the sum of
            all file sizes in basebackup directory, is used to calculate the
            ratio. For ``postgres`` backups, the :attr:`cluster_size` is used, which contains
            the estimated size of the Postgres cluster at backup time.

            We perform this calculation to make an estimation of how much network and disk
            I/O has been saved when taking an incremental backup through ``rsync`` or through
            ``pg_basebackup``.

            We abuse of the term "deduplication" here. It makes more sense to ``rsync`` than to
            ``postgres`` method. However, the idea is the same in both cases: get an estimation
            of resources saving.

        .. note::
            Even though this property is available in this base class, it is not
            expected to be used in the context of cloud backups.

        :return float: The backup deduplication ratio.
        """
        size = self.cluster_size
        if self.backup_type == "rsync":
            size = self.size
        if size and self.deduplicated_size:
            return 1 - (self.deduplicated_size / size)
        return 0

    def to_dict(self):
        """
        Return the backup_info content as a simple dictionary

        :return dict:
        """
        result = dict(self.items())
        top_level_fields = (
            "backup_id",
            "server_name",
            "mode",
            "tablespaces",
            "included_files",
            "copy_stats",
            "snapshots_info",
        )
        for field_name in top_level_fields:
            field_value = getattr(self, field_name)
            if field_value is not None or field_name not in self._hide_if_null:
                result.update({field_name: field_value})
        if self.snapshots_info is not None:
            result.update({"snapshots_info": self.snapshots_info.to_dict()})
        return result

    def to_json(self):
        """
        Return an equivalent dictionary that uses only json-supported types
        """
        data = self.to_dict()
        # Convert fields which need special types not supported by json
        if data.get("tablespaces") is not None:
            data["tablespaces"] = [list(item) for item in data["tablespaces"]]
        # Note on the `begin_time_iso`` and `end_time_iso`` fields:
        # as ctime is not timezone-aware and mostly for human-readable output,
        # we want to migrate to isoformat for datetime objects representation.
        # To retain, for now, compatibility with the previous version of the output
        # we add two new _iso fields to the json document.
        if data.get("begin_time") is not None:
            begin_time = data["begin_time"]
            data["begin_time"] = begin_time.ctime()
            data["begin_time_iso"] = begin_time.isoformat()
        if data.get("end_time") is not None:
            end_time = data["end_time"]
            data["end_time"] = end_time.ctime()
            data["end_time_iso"] = end_time.isoformat()
        return data

    @classmethod
    def from_json(cls, server, json_backup_info):
        """
        Factory method that builds a BackupInfo object
        from a json dictionary

        :param barman.Server server: the server related to the Backup
        :param dict json_backup_info: the data set containing values from json
        """
        data = dict(json_backup_info)
        # Convert fields which need special types not supported by json
        if data.get("tablespaces") is not None:
            data["tablespaces"] = [
                Tablespace._make(item) for item in data["tablespaces"]
            ]
        if data.get("begin_time_iso") is not None:
            data["begin_time"] = load_datetime_tz(data["begin_time_iso"])
            del data["begin_time_iso"]
        elif data.get("begin_time") is not None:
            data["begin_time"] = load_datetime_tz(data["begin_time"])
        if data.get("end_time_iso") is not None:
            data["end_time"] = load_datetime_tz(data["end_time_iso"])
            del data["end_time_iso"]
        elif data.get("end_time") is not None:
            data["end_time"] = load_datetime_tz(data["end_time"])
        # Instantiate a BackupInfo object using the converted fields
        return cls(server, **data)

    def pg_major_version(self):
        """
        Returns the major version of the PostgreSQL instance from which the
        backup was made taking into account the change in versioning scheme
        between PostgreSQL < 10.0 and PostgreSQL >= 10.0.
        """
        major = int(self.version / 10000)
        if major < 10:
            minor = int(self.version / 100 % 100)
            return "%d.%d" % (major, minor)
        else:
            return str(major)

    def wal_directory(self):
        """
        Returns "pg_wal" (v10 and above) or "pg_xlog" (v9.6 and below) based on
        the Postgres version represented by this backup
        """
        return "pg_wal" if self.version >= 100000 else "pg_xlog"


class LocalBackupInfo(BackupInfo):
    __slots__ = "server", "config", "backup_manager"

    def __init__(self, server, info_file=None, backup_id=None, **kwargs):
        """
        Stores meta information about a single backup

        :param Server server:
        :param file,str,None info_file:
        :param str,None backup_id:
        :raise BackupInfoBadInitialisation: if the info_file content is invalid
            or neither backup_info or
        """
        # Initialises the attributes for the object
        # based on the predefined keys
        super(LocalBackupInfo, self).__init__(backup_id=backup_id, **kwargs)

        self.server = server
        self.config = server.config
        self.backup_manager = self.server.backup_manager
        self.server_name = self.config.name
        self.mode = self.backup_manager.mode
        if backup_id:
            # Cannot pass both info_file and backup_id
            if info_file:
                raise BackupInfoBadInitialisation(
                    "both info_file and backup_id parameters are set"
                )
            self.backup_id = backup_id
            self.filename = self.get_filename()
            # Check if a backup info file for a given server and a given ID
            # already exists. If so load the values from the file.
            if os.path.exists(self.filename):
                self.load(filename=self.filename)
        elif info_file:
            if hasattr(info_file, "read"):
                # We have been given a file-like object
                self.load(file_object=info_file)
            else:
                # Just a file name
                self.load(filename=info_file)
            self.backup_id = self.detect_backup_id()
        elif not info_file:
            raise BackupInfoBadInitialisation(
                "backup_id and info_file parameters are both unset"
            )
        # Manage backup version for new backup structure
        try:
            # the presence of pgdata directory is the marker of version 1
            if self.backup_id is not None and os.path.exists(
                os.path.join(self.get_basebackup_directory(), "pgdata")
            ):
                self.backup_version = 1
        except Exception as e:
            _logger.warning(
                "Error detecting backup_version, use default: 2. Failure reason: %s",
                e,
            )

    def get_list_of_files(self, target):
        """
        Get the list of files for the current backup
        """
        # Walk down the base backup directory
        if target in ("data", "standalone", "full"):
            for root, _, files in os.walk(self.get_basebackup_directory()):
                files.sort()
                for f in files:
                    yield os.path.join(root, f)
        if target in "standalone":
            # List all the WAL files for this backup
            for x in self.get_required_wal_segments():
                yield self.server.get_wal_full_path(x)
        if target in ("wal", "full"):
            for wal_info in self.server.get_wal_until_next_backup(
                self, include_history=True
            ):
                yield wal_info.fullpath(self.server)

    def detect_backup_id(self):
        """
        Detect the backup ID from the file name or parent directory name.

        .. note::
            The ``backup.info`` file was relocated and renamed in version 3.13.2 to
            also contain the backup id as a prefix. In previous versions, the parent
            directory of the file was named with the backup id. This method handles
            both cases.
        """
        if self.filename:
            match = re.match(r"^(.+?)-backup\.info$", os.path.basename(self.filename))
            if match:
                return match.group(1)
            return os.path.basename(os.path.dirname(self.filename))

    def get_basebackup_directory(self):
        """
        Get the default filename for the backup.info file based on
        backup ID and server directory for base backups
        """
        return os.path.join(self.config.basebackups_directory, self.backup_id)

    def get_data_directory(self, tablespace_oid=None):
        """
        Get path to the backup data dir according with the backup version

        If tablespace_oid is passed, build the path to the tablespace
        base directory, according with the backup version

        :param int tablespace_oid: the oid of a valid tablespace
        """
        # Check if a tablespace oid is passed and if is a valid oid
        if tablespace_oid is not None:
            if self.tablespaces is None:
                raise ValueError("Invalid tablespace OID %s" % tablespace_oid)

            invalid_oid = all(
                str(tablespace_oid) != str(tablespace.oid)
                for tablespace in self.tablespaces
            )
            if invalid_oid:
                raise ValueError("Invalid tablespace OID %s" % tablespace_oid)

        # Build the requested path according to backup_version value
        path = [self.get_basebackup_directory()]
        # Check the version of the backup
        if self.backup_version == 2:
            # If an oid has been provided, we are looking for a tablespace
            if tablespace_oid is not None:
                # Append the oid to the basedir of the backup
                path.append(str(tablespace_oid))
            else:
                # Looking for the data dir
                path.append("data")
        else:
            # Backup v1, use pgdata as base
            path.append("pgdata")
            # If a oid has been provided, we are looking for a tablespace.
            if tablespace_oid is not None:
                # Append the path to pg_tblspc/oid folder inside pgdata
                path.extend(("pg_tblspc", str(tablespace_oid)))
        # Return the built path
        return os.path.join(*path)

    def get_filename(self, write=False):
        """
        Get the default file path for the backup.info file.

        :param bool write: if the file is to be written or not.

        .. note::
            The ``backup.info`` file was relocated and renamed in version 3.13.2 to
            also contain the backup id as a prefix. In previous versions, it lived
            inside the backup directory alongside the data directory. This method
            handles both paths.

            When writing, always write to the new path. When reading, it could be that
            a user just migrated to the new version, so we also handle reading from the
            old path.
        """
        path = os.path.join(self.server.meta_directory, f"{self.backup_id}-backup.info")
        old_path = os.path.join(self.get_basebackup_directory(), "backup.info")
        if write or os.path.exists(path) or not os.path.exists(old_path):
            return path
        return old_path

    def save(self, filename=None, file_object=None):
        # Update the filename before saving to ensure we're using the correct value
        # It could be that a user just migrated to version 3.13.2, which relocated the
        # backup.info path. In this case, the file was read from the old path but has
        # to be written to the new path
        if not filename:
            self.filename = self.get_filename(write=True)
        if not file_object:
            # Make sure the containing directory exists
            filename = filename or self.filename
            dir_name = os.path.dirname(filename)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
        super(LocalBackupInfo, self).save(filename=filename, file_object=file_object)

    def get_backup_manifest_path(self):
        """
        Get the full path to the backup manifest file

        :return str: the full path to the backup manifest file.
        """
        return os.path.join(self.get_data_directory(), "backup_manifest")

    def get_parent_backup_info(self):
        """
        If the backup is incremental, build the :class:`LocalBackupInfo` object
        for the parent backup and return it.
        If the backup is not incremental OR the status of the parent
        backup is ``EMPTY``, return ``None``.

        :return LocalBackupInfo|None: the parent backup info object,
            or None if it does not exist or is empty.
        """
        if self.is_incremental:
            backup_info = LocalBackupInfo(
                self.server,
                backup_id=self.parent_backup_id,
            )

            if backup_info.status != BackupInfo.EMPTY:
                return backup_info

        return None

    def get_child_backup_info(self, child_backup_id):
        """
        Allow to retrieve a specific child of the current incremental backup,
        if there are any.

        If the child backup exists, the LocalBackupInfo object for it is returned.
        If does not exist or its status is `EMPTY`, return None

        :param str child_backup_id: the ID of the child backup to retrieve

        :return LocalBackupInfo|None: the child backup info object,
            or None if it does not exist or is empty.
        """
        if self.children_backup_ids:
            if child_backup_id in self.children_backup_ids:
                backup_info = LocalBackupInfo(
                    self.server,
                    backup_id=child_backup_id,
                )

                if backup_info.status != BackupInfo.EMPTY:
                    return backup_info

        return None

    def walk_backups_tree(self, return_self=True):
        """
        Walk through all the children backups of the current backup.

        .. note::
            The objects are returned with a bottom-up approach, including all
            children backups plus the caller backup.

        :param bool return_self: Whether to return the current backup.
            Default to ``True``.

        :yields: a generator of :class:`LocalBackupInfo` objects for each
            backup, walking from the leaves to self.
        """

        if self.children_backup_ids:
            for child_backup_id in self.children_backup_ids:
                backup_info = LocalBackupInfo(
                    self.server,
                    backup_id=child_backup_id,
                )
                yield from backup_info.walk_backups_tree()
        if not return_self:
            return
        yield self

    def walk_to_root(self, return_self=True):
        """
        Walk through all the parent backups of the current backup.

        .. note::
            The objects are returned with a bottom-up approach, including all
            parents backups plus the caller backup if *return_self* is ``True``.

        :param bool return_self: Whether to return the current backup.
            Default to ``True``.

        :yield: a generator of :class:`LocalBackupInfo` objects for each parent backup.
        """

        if return_self:
            yield self
        backup_info = self.get_parent_backup_info()
        while backup_info:
            yield backup_info
            backup_info = backup_info.get_parent_backup_info()

    def is_checksum_consistent(self):
        """
            Check if all backups in the chain are consistent with their checksums
            configurations.

            The backup chain is considered inconsistent if the current backup was taken with
            ``data_checksums`` enabled and any of its ascendants were taken with it disabled.
            It is considered consistent otherwise.

        .. note::
            While this method was created to check inconsistencies in chains of one
            or more (Postgres 17+ core) incremental backups, it can be safely used
            with any Postgres version and with any Barman backup method. That is
            true because it always returns ``True`` when called for a Postgres full
            backup or for a rsync backup.

            :return bool: ``True`` if it is consistent, ``False`` otherwise.
        """
        if self.data_checksums != "on" or not self.is_incremental:
            return True
        for backup in self.walk_to_root(return_self=False):
            if backup.data_checksums == "off":
                return False
        return True

    @property
    def is_full(self):
        """
        Check if this is a full backup.

        :return bool: ``True`` if it's a full backup or ``False`` if not.
        """
        return self.backup_type not in ("snapshot", "incremental")

    @property
    def is_orphan(self):
        """
        Determine if the backup is an orphan.

        An orphan backup is defined as a backup directory that contains only
        a non-empty backup.info file. This may indicate an incomplete delete operation.

        :return bool: ``True`` if the backup is an orphan, ``False`` otherwise.

        .. note::
            The ``backup.info`` file was relocated and renamed in version 3.13.2 to
            also contain the backup id as a prefix. In previous versions, the parent
            directory of the file was named with the backup id. This method handles
            both cases.
        """
        if self.status == BackupInfo.EMPTY:
            return False
        backup_dir = self.get_basebackup_directory()
        # In the >= 3.13.2 structure, it is considered orphan if the backup.info exists
        # in the server meta directory while no directory related to the backup exists
        backup_info_path = self.get_filename()
        if os.path.exists(backup_info_path) and not os.path.exists(backup_dir):
            return True
        # In the < 3.13.2 structure, the backup.info lived inside the backup directory.
        # In this case, it is considered orphan if the backup.info exists alone in there
        old_backup_info_path = os.path.join(backup_dir, "backup.info")
        if os.path.exists(backup_dir) and os.path.exists(old_backup_info_path):
            if len(os.listdir(backup_dir)) == 1:
                return True
        return False


class SyntheticBackupInfo(LocalBackupInfo):
    def __init__(
        self, server, base_directory, backup_id=None, info_file=None, **kwargs
    ):
        """
        Stores meta information about a single synthetic backup.

        .. note::
            A synthetic backup is a base backup which was artificially created
            through ``pg_combinebackup``. A synthetic backup is not part of
            the Barman backup catalog, and only exists so we are able to
            recover a backup created by ``pg_combinebackup`` utility, as
            almost all functions and methods require a backup info object.

        The only difference from this class to its parent :class:`LocalBackupInfo`
        is that it accepts a custom base directory for the backup as synthetic
        backups are expected to live on directories other than the default
        ``<server_name>/base`` path.

        :param barman.server.Server server: the server that owns the
            synthetic backup
        :param str base_directory: the root directory where this synthetic
            backup resides, essentially an override to the
            ``server.config.basebackups_directory`` configuration.
        :param str|None backup_id: the backup id of this backup
        :param None|str|TextIO info_file: path or file descriptor of an existing
            synthetic ``backup.info`` file
        """
        self.base_directory = base_directory
        super(SyntheticBackupInfo, self).__init__(
            server, info_file, backup_id, **kwargs
        )

    def get_basebackup_directory(self):
        """Get the backup directory based on its base directory"""
        return os.path.join(self.base_directory, self.backup_id)


class VolatileBackupInfo(LocalBackupInfo):
    def __init__(
        self, server, base_directory, backup_id=None, info_file=None, **kwargs
    ):
        """
        A class to hold temporary, in-memory updates on top of a
        :class:`LocalBackupInfo` object.

        This class allows modification of certain fields of a :class:`LocalBackupInfo`
        object without affecting the actual backup file on disk. It is designed to
        support methods like :meth:`RecoveryExecutor._decrypt_backup`, where custom,
        situational updates to backup information are necessary for processing, such as
        custom tablespace mappings during backup decompression or combination.

        The :class:`VolatileBackupInfo` does not persist changes to the disk, ensuring
        that only in-memory instances are updated, while the original
        :class:`BackupInfo` remains unaltered.

        :param barman.server.Server server: The server of the
            :class:`LocalBackupInfo`.
        :param str base_directory: The directory where this backup is stored,
            essentially an override to the
            :attr:`barman.config.ServerConfig.basebackups_directory` configuration
            option.
        :param str|None backup_id: The backup id of the backup.
        :param None|str|TextIO info_file: The path to an existing ``backup.info`` file,
        or
            a file-like object from which to read the backup information.
        """
        self.base_directory = base_directory
        super(VolatileBackupInfo, self).__init__(server, info_file, backup_id, **kwargs)

    def get_basebackup_directory(self):
        """
        Retrieve the full path to the base backup directory for this backup.

        This method constructs the path to the directory where the backup is stored
        based on the provided :attr:`base_directory` and :attr:`backup_id`. It is
        particularly useful for scenarios where the backup directory is customized or
        overridden.

        :return str: The full path to the base backup directory.
        """
        return os.path.join(self.base_directory, self.backup_id)

    def save(self, *args, **kwargs):
        """
        Override the save method to prevent its usage.

        :raises NotImplementedError: This method is not implemented.
        """
        raise NotImplementedError("The save method is not implemented.")
