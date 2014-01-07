# Copyright (C) 2013 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import os
from barman.compression import identify_compression


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

    def save(self, filename=None):
        """
        Serialize the object to the specified file

        If the filename is not specified it uses the one memorized in the
        filename attribute. If neither the filename attribute and parameter are
        set a ValueError exception is raised.

        :param str filename: the file to write
        :raises: ValueError
        """
        filename = filename or self.filename
        if not filename:
            raise ValueError('filename was not specified in any way')

        with open(filename, 'w') as info:
            for name in sorted(vars(type(self))):
                field = getattr(type(self), name)
                value = getattr(self, name, None)
                if isinstance(field, Field):
                    if callable(field.to_str):
                        value = field.to_str(value)
                    info.write("%s=%s\n" % (name, value))

    def load(self, filename):
        """
        Replaces the current object content with the one deserialized from
        the provided file.

        This method set the filename attribute.

        A ValueError exception is raised if the provided file contains any
        invalid line.

        :param str filename: the file to read
        :raises: ValueError
        """
        self.filename = filename
        with open(filename, 'r') as info:
            for line in info:
                if line.isspace() or line.rstrip().startswith('#'):
                    continue

                try:
                    name, value = [x.strip() for x in line.split('=', 1)]
                except:
                    raise ValueError('invalid line %s in file %s' % (
                        line.strip(), filename))

                field = getattr(type(self), name, None)
                if value == 'None':
                    value = None
                if isinstance(field, Field) and callable(field.from_str):
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
    time = Field('time', load=int, doc='WAL file modification time')
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