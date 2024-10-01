.. _commands-barman-switch-wal:

``barman switch-wal``
"""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    switch-wal
        [ --archive ]
        [ --archive-timeout ]
        [ --force ]
        SERVER_NAME
    

Description
^^^^^^^^^^^

Execute ``pg_switch_wal()`` on the target server (Postgres versions 10 and later) or
``pg_switch_xlog()`` (for Postgres versions 8.3 to 9.6).

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``--archive``
    Waits for one WAL file to be archived. If no WAL file is archived within a specified
    time (default: ``30`` seconds), Barman will terminate with a failure exit code. This
    option is also available on standby servers.

``--archive-timeout``
    Specify the amount of time in seconds (default: ``30`` seconds) that the archiver
    will wait for a new WAL file to be archived before timing out. This option is also
    available on standby servers.

``--force``
    Forces the switch by executing a CHECKPOINT before ``pg_switch_wal()``.
    
    .. note::
        Running a CHECKPOINT may increase I/O load on the PostgreSQL server, so use this
        option cautiously.
