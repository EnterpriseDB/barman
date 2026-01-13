.. _wal_archiving:

WAL archiving
=============

Barman also offers additional features regarding WAL archiving.

.. _wal_archiving-wal-compression:

WAL compression
---------------

Barman can compress WAL files as they enter the Barman's WAL archive. This process is
handled automatically by ``barman cron`` or when the ``barman archive-wal`` command is
executed manually.

Compression is enabled via the ``compression`` option in the configuration file.
The option can use one of the following values:

* ``lz4``: for LZ4 compression (requires the ``lz4`` library to be installed);
* ``xz``: for XZ compression (uses Python's internal compression library);
* ``zstd``: for Zstandard compression (requires the ``zstandard`` library to be
  installed);
* ``gzip``: for Gzip compression (uses Python's internal compression library);
* ``pygzip``: for Gzip compression (currently the same as ``gzip``)(Deprecated);
* ``pigz``: for Pigz compression (requires the ``pigz`` utility);
* ``bzip2``: for Bzip2 compression (uses Python's internal compression library);
* ``pybzip2``: for Bzip2 compression (currently the same as ``bzip2``)(Deprecated);
* ``custom``: for custom compression, which requires you to set the following options
  as well: ``custom_compression_filter``, ``custom_decompression_filter``,
  ``custom_compression_magic``. Check :ref:`configuration-options-wals` for details.
  (Deprecated)

.. deprecated:: 3.16
    The ``pygzip`` and ``pybzip2`` compression options are deprecated and will be
    removed in a future release. Use their equivalents ``gzip`` and ``bzip2`` instead.

.. deprecated:: 3.17
    The ``custom`` compression option and related configuration options
    (``custom_compression_filter``, ``custom_decompression_filter``, and
    ``custom_compression_magic``) are deprecated and will be removed in a future
    release. Use one of the built-in compression algorithms instead.

.. note::
    When using ``pigz`` Barman forks a new process for compression.

.. important::
    Barman does not store metadata on how each WAL file was compressed, nor on how to
    decompress them. It relies mainly on the server's current configuration. Therefore,
    when using ``compression = custom``, you must ensure that
    ``custom_decompression_filter`` remains compatible with the WALs currently
    available in archive. If, for instance, ``custom_decompression_filter`` is changed
    to a different compression algorithm midway through, Barman will be unable to
    decompress old WALs still archived with the previous compression algorithm when
    restoring.

A compression level can also be specified by using the ``compression_level``
configuration. Valid values are integers within the supported range of the chosen
algorithm or one of the predefined labels: ``low``, ``medium``, and ``high``. The range
of each algorithm as well as what level each predefined label maps to can be found in
:ref:`compression_level <configuration-options-compression-level>`.

If archiving via ``archive_command`` with ``barman-wal-archive``, compression
can also be configured using the ``--compression`` and ``--compression-level`` options
with an algorithm and level of your choice. In such cases, compression is done on the
client side, before the WAL is sent to the Barman server.
See :ref:`commands-barman-cli-barman-wal-archive`.

.. note::
  When compression is enabled in ``barman-wal-archive``, it takes precedence over the
  compression settings configured on the Barman server, if they differ.

.. important::
  When compression is enabled in ``barman-wal-archive``, it is performed on the client
  side, before the file is sent to Barman. Be mindful of the database server's load and
  the chosen compression algorithm and level, as higher compression can delay WAL
  shipping, causing WAL files to accumulate.


.. _wal_archiving-WAL-encryption:

WAL encryption
--------------

Barman can encrypt WAL files as they enter the Barman's WAL archive. This process is
handled automatically by ``barman cron`` or when the ``barman archive-wal`` command is
executed manually.

Both WAL and backup encryption are enabled via the ``encryption`` option in the
configuration file. Currently, only ``gpg`` and ``none`` (no encryption) are accepted
as values. A detailed guide on how to configure encryption is available in
:ref:`backup-encryption`.


.. _wal_archiving-synchronous-WAL-streaming:

Synchronous WAL streaming
-------------------------

Barman can also reduce the :term:`RPO` to zero, by collecting the transaction WAL files
like a synchronous standby server would.

To configure a scenario with :term:`RPO` zero, the Barman server must be configured to archive WALs via
a streaming connection and the receive-wal process has to be configured as a
synchronous connection to the Postgres server.

First, you need to retrieve the application name of the Barman receive-wal process with the 
``show-servers`` command:

.. code-block:: bash

  barman show-servers pg | grep streaming_archiver_name

Output:

.. code-block:: text

  streaming_archiver_name: barman_receive_wal

.. note::

  The application name Barman uses when starting the receive-wal process is configured
  with the ``streaming_archiver_name`` configuration option. The default value for this
  option is ``barman_receive_wal``.

Then the application name should be added to the ``synchronous_standby_names``
parameter in the ``postgresql.conf`` file:

.. code-block:: bash

  synchronous_standby_names = 'barman_receive_wal'


.. important::

  Barman with :term:`RPO` zero adds more security to your backups and gives you more
  recovery options. However, it should not be considered as a substitution of a
  real Postgres replica. Please read the `official Postgres documentation about
  "Syncronous Replication" <https://www.postgresql.org/docs/current/runtime-config-replication.html>`_
  for more information on this topic.

The Postgres server configuration needs to be reloaded for the changes to take effect.

If the server has been configured correctly, the ``barman replication-status`` command
should show the receive-wal process as a synchronous streaming client:

.. code-block:: bash

  barman replication-status pg

Output:

.. code-block:: text

  Status of streaming clients for server 'pg':
    Current xlog location on master: 0/9000098
    Number of streaming clients: 1

    1. #1 Sync WAL streamer
      Application name: barman_receive_wal
      Sync stage      : 3/3 Remote write
      Communication   : TCP/IP
      IP Address      : 139.59.135.32 / Port: 58262 / Host: -
      User name       : streaming_barman
      Current state   : streaming (sync)
      Replication slot: barman
      WAL sender PID  : 2501
      Started at      : 2016-09-16 10:33:01.725883+00:00
      Sent location   : 0/9000098 (diff: 0 B)
      Write location  : 0/9000098 (diff: 0 B)
      Flush location  : 0/9000098 (diff: 0 B)
