# Copyright (C) 2013-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import os
import dateutil.parser
import dateutil.tz
import collections
from barman import xlog
from barman.compression import identify_compression

# create a namedtuple object called Tablespace with 'name' 'oid' and 'location'
# as property.
Tablespace = collections.namedtuple('Tablespace', 'name oid location')


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
        if not hasattr(obj, '_fields'):
            obj._fields = {}
        return obj._fields.setdefault(self.name, self.default)

    def __set__(self, obj, value):
        if not hasattr(obj, '_fields'):
            obj._fields = {}
        obj._fields[self.name] = value

    def __delete__(self, obj):
        raise AttributeError("can't delete attribute")

    def dump(self, to_str):
        return type(self)(self.name, to_str, self.from_str, self.__doc__)

    def load(self, from_str):
        return type(self)(self.name, self.to_str, from_str, self.__doc__)


class FieldListFile(object):

    __slots__ = ('_fields', 'filename')

    def __init__(self, **kwargs):
        """
        Represent a predefined set of keys with the associated value.

        The constructor build the object assigning every keyword argument to
        the corresponding attribute. If a provided keyword argument doesn't
        has a corresponding attribute an AttributeError exception is raised.

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
                raise AttributeError('unknown attribute %s' % name)

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
                info = open(filename, 'w')
            else:
                info = None

        if not info:
            raise ValueError(
                'either a valid filename or a file_object must be specified')

        with info:
            for name in sorted(vars(type(self))):
                field = getattr(type(self), name)
                value = getattr(self, name, None)
                if isinstance(field, Field):
                    if callable(field.to_str):
                        value = field.to_str(value)
                    info.write("%s=%s\n" % (name, value))

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
            info = open(filename, 'r')
        else:
            raise ValueError(
                'either filename or file_object must be specified')

        # detect the filename if a file_object is passed
        if not filename and file_object:
            if hasattr(file_object, 'name'):
                filename = file_object.name

        # canonicalize filename
        if filename:
            self.filename = os.path.abspath(filename)
        else:
            self.filename = None
            filename = '<UNKNOWN>'  # This is only for error reporting

        with info:
            for line in info:
                # skip spaces and comments
                if line.isspace() or line.rstrip().startswith('#'):
                    continue

                # parse the line of form "key = value"
                try:
                    name, value = [x.strip() for x in line.split('=', 1)]
                except ValueError:
                    raise ValueError('invalid line %s in file %s' % (
                        line.strip(), filename))

                # use the from_str function to parse the value
                field = getattr(type(self), name, None)
                if value == 'None':
                    value = None
                elif isinstance(field, Field) and callable(field.from_str):
                        value = field.from_str(value)
                setattr(self, name, value)

    def items(self):
        """
        Return a generator returning a list of (key, value) pairs.

        If a filed has a dump function defined, it will be used.
        """
        for name in sorted(vars(type(self))):
            field = getattr(type(self), name)
            value = getattr(self, name, None)
            if isinstance(field, Field):
                if callable(field.to_str):
                    value = field.to_str(value)
                yield (name, value)

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            ', '.join(['%s=%r' % x for x in self.items()]))


class WalFileInfo(FieldListFile):
    """
    Metadata of a WAL file.
    """

    __slots__ = ()

    name = Field('name', doc='base name of WAL file')
    full_path = Field('full_path', doc='complete path of the file')
    size = Field('size', load=int, doc='WAL file size after compression')
    time = Field('time', load=float, doc='WAL file modification time '
                                         '(seconds since epoch)')
    compression = Field('compression', doc='compression type')

    @classmethod
    def from_file(cls, filename, default_compression=None, **kwargs):
        """
        Factory method to generate a WalFileInfo from a WAL file.

        Every keyword argument will override any attribute from the provided
        file. If a keyword argument doesn't has a corresponding attribute
        an AttributeError exception is raised.

        :param str filename: the file to inspect
        :param str default_compression: the compression to set if
            the current schema is not identifiable.
        """
        stat = os.stat(filename)
        kwargs.setdefault('name', os.path.basename(filename))
        kwargs.setdefault('full_path', os.path.abspath(filename))
        kwargs.setdefault('size', stat.st_size)
        kwargs.setdefault('time', stat.st_mtime)
        if 'compression' not in kwargs:
            kwargs['compression'] = identify_compression(filename) \
                or default_compression
        obj = cls(**kwargs)
        obj.filename = "%s.meta" % filename
        return obj

    def to_xlogdb_line(self):
        """
        Format the content of this object as a xlogdb line.
        """
        return "%s\t%s\t%s\t%s\n" % (
            self.name,
            self.size,
            self.time,
            self.compression)

    @classmethod
    def from_xlogdb_line(cls, server, line):
        """
        Parse a line from xlog catalogue

        :param Server server: server object
        :param str line: a line in the wal database to parse
        :rtype: WalFileInfo
        """
        try:
            name, size, time, compression = line.split()
        except ValueError:
            # Old format compatibility (no compression)
            compression = None
            try:
                name, size, time = line.split()
            except ValueError:
                raise ValueError("cannot parse line: %r" % (line,))
        # The to_xlogdb_line method writes None values as literal 'None'
        if compression == 'None':
            compression = None
        full_path = server.get_wal_full_path(name)
        size = int(size)
        time = float(time)
        return cls(name=name, full_path=full_path, size=size, time=time,
                   compression=compression)


class UnknownBackupIdException(Exception):
    """
    The searched backup_id doesn't exists
    """


class BackupInfoBadInitialisation(Exception):
    """
    Exception for a bad initialization error
    """


class BackupInfo(FieldListFile):

    #: Conversion to string
    EMPTY = 'EMPTY'
    STARTED = 'STARTED'
    FAILED = 'FAILED'
    DONE = 'DONE'
    STATUS_ALL = (EMPTY, STARTED, DONE, FAILED)
    STATUS_NOT_EMPTY = (STARTED, DONE, FAILED)

    #: Status according to retention policies
    OBSOLETE = 'OBSOLETE'
    VALID = 'VALID'
    POTENTIALLY_OBSOLETE = 'OBSOLETE*'
    NONE = '-'
    RETENTION_STATUS = (OBSOLETE, VALID, POTENTIALLY_OBSOLETE, NONE)

    version = Field('version', load=int)
    pgdata = Field('pgdata')
    # Parse the tablespaces as a literal Python list of namedtuple
    # Output the tablespaces as a literal Python list of tuple
    tablespaces = Field('tablespaces', load=load_tablespace_list,
                        dump=output_tablespace_list)
    # Timeline is an integer
    timeline = Field('timeline', load=int)
    begin_time = Field('begin_time', load=load_datetime_tz)
    begin_xlog = Field('begin_xlog')
    begin_wal = Field('begin_wal')
    begin_offset = Field('begin_offset')
    size = Field('size', load=int)
    end_time = Field('end_time', load=load_datetime_tz)
    end_xlog = Field('end_xlog')
    end_wal = Field('end_wal')
    end_offset = Field('end_offset')
    status = Field('status', default=EMPTY)
    server_name = Field('server_name')
    error = Field('error')
    mode = Field('mode')
    config_file = Field('config_file')
    hba_file = Field('hba_file')
    ident_file = Field('ident_file')
    backup_label = Field('backup_label', load=ast.literal_eval, dump=repr)

    __slots__ = ('server', 'config', 'backup_manager', 'backup_id')

    def __init__(self, server, info_file=None, backup_id=None, **kwargs):
        # Initialises the attributes for the object based on the predefined keys
        """
        Stores meta information about a single backup

        :param Server server:
        :param file,str,None info_file:
        :param str,None backup_id:
        :raise BackupInfoBadInitialisation: if the info_file content is invalid
            or neither backup_info or
        """

        super(BackupInfo, self).__init__(**kwargs)

        self.server = server
        self.config = server.config
        self.backup_manager = self.server.backup_manager
        if backup_id:
            # Cannot pass both info_file and backup_id
            if info_file:
                raise BackupInfoBadInitialisation(
                    'both info_file and backup_id parameters are set')
            self.backup_id = backup_id
            self.filename = self.get_filename()
            # Check if a backup info file for a given server and a given ID
            # already exists. If not, create it from scratch
            if not os.path.exists(self.filename):
                self.server_name = self.config.name
                self.mode = self.backup_manager.name
            else:
                self.load(filename=self.filename)
        elif info_file:
            if hasattr(info_file, 'read'):
            # We have been given a file-like object
                self.load(file_object=info_file)
            else:
                # Just a file name
                self.load(filename=info_file)
            self.backup_id = self.detect_backup_id()
        elif not info_file:
            raise BackupInfoBadInitialisation(
                'backup_id and info_file parameters are both unset')

    def get_required_wal_segments(self):
        """
        Get the list of required WAL segments for the current backup
        """
        return xlog.enumerate_segments(self.begin_wal, self.end_wal,
                                       self.version)

    def get_list_of_files(self, target):
        """
        Get the list of files for the current backup
        """
        # Walk down the base backup directory
        if target in ('data', 'standalone', 'full'):
            for root, _, files in os.walk(self.get_basebackup_directory()):
                for f in files:
                    yield os.path.join(root, f)
        if target in 'standalone':
            # List all the WAL files for this backup
            for x in self.get_required_wal_segments():
                yield self.server.get_wal_full_path(x)
        if target in ('wal', 'full'):
            for wal_info in self.server.get_wal_until_next_backup(self):
                yield wal_info.full_path

    def detect_backup_id(self):
        """
        Detect the backup ID from the name of the parent dir of the info file
        """
        if self.filename:
            return os.path.basename(os.path.dirname(self.filename))
        else:
            return None

    def get_basebackup_directory(self):
        """
        Get the default filename for the backup.info file based on
        backup ID and server directory for base backups
        """
        return os.path.join(self.config.basebackups_directory,
                            self.backup_id)

    def get_filename(self):
        """
        Get the default filename for the backup.info file based on
        backup ID and server directory for base backups
        """
        return os.path.join(self.get_basebackup_directory(), 'backup.info')

    def set_attribute(self, key, value):
        """
        Set a value for a given key
        """
        setattr(self, key, value)

    def save(self, filename=None, file_object=None):
        if not file_object:
            # Make sure the containing directory exists
            filename = filename or self.filename
            dir_name = os.path.dirname(filename)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
        super(BackupInfo, self).save(filename=filename, file_object=file_object)

    def to_dict(self):
        """
        Return the backup_info content as a simple dictionary

        :return dict:
        """
        result = dict(self.items())
        result.update(backup_id=self.backup_id, server_name=self.server_name,
                      mode=self.mode, tablespaces=self.tablespaces)
        return result
