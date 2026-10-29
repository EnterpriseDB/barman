.. _commands-barman-export-backup:

``barman export-backup``
""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block::

    export-backup
        [ { -h | --help } ]
        [ --compression { gzip | bzip2 | xz } ]
        [ --compression-level COMPRESSION_LEVEL ]
        SERVER_NAME BACKUP_ID OUTPUT_DIRECTORY

Description
^^^^^^^^^^^

Export the specified backup as a single tarball, suitable for hand-off to an
external archival system. The tarball is self-contained: it bundles the
backup data files, any tablespaces, the WAL segments required to bring the
backup to a consistent state, and the Barman metadata needed to re-register
the backup with :ref:`commands-barman-import-backup` later.

The generated file is named using the pattern
``backup-export-SERVER_NAME-BACKUP_ID-TIMESTAMP-CHECKSUM.tar[.gz|.bz2|.xz]``.
The filename is part of the integrity contract — do not rename the tarball,
or :ref:`commands-barman-import-backup` will refuse it.

Only full backups in ``DONE`` state on a server using local storage can be
exported. See :ref:`user_guide-export-import` for the full feature
description.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``BACKUP_ID``
    ID or name of the backup in the barman catalog. If more than one backup has the
    same name, the command fails and lists the matching backup IDs.

``OUTPUT_DIRECTORY``
    Existing directory where the exported tarball will be written. Must be
    writable and executable by the user running Barman.

``--compression {gzip | bzip2 | xz}``
    Compress the outer tarball using the given algorithm. By default the
    tarball is not compressed. Compression applies only to the outer
    tarball; data files and WAL segments stored inside are not recompressed,
    and any pre-existing WAL compression is preserved through the
    associated ``xlog.db`` metadata.

``--compression-level COMPRESSION_LEVEL``
    Compression level to pass to the chosen algorithm. Valid levels are
    ``1``-``9`` for ``gzip`` and ``bzip2``, and ``0``-``9`` for ``xz``.
    Defaults to the algorithm's own default level. Requires
    ``--compression`` to be set.

    .. note::

       Passing an explicit ``--compression-level`` requires Python 3.12 or
       newer for ``gzip``/``bzip2`` and Python 3.14 or newer for ``xz``. On
       older interpreters the option is accepted, a warning is emitted, and
       the algorithm's default level is used instead.