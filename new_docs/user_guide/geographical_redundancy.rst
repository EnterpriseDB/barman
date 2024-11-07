.. _geographical-redundancy:

Geographical Redundancy
=======================

It's possible to set up cascading backup architectures with Barman, where the
source of a Barman backup server is another Barman installation rather than a
PostgreSQL server.

This feature allows users to transparently keep geographically distributed
copies of PostgreSQL backups.

In Barman jargon, a Barman backup server that is connected to another Barman
installation, rather than a PostgreSQL server, is defined as a passive node.
A passive node is configured through the ``primary_ssh_command`` option, available
both at global (for a full replica of a primary Barman installation) and server
level (for mixed scenarios, having both direct and passive servers).

Sync information
----------------

The ``barman sync-info`` command is used to collect information regarding the
current status of a Barman server which is useful for synchronization purposes.
The available syntax is the following:

.. code-block:: bash
	
	barman sync-info [--primary] <server_name> [<last_wal> [<last_position>]]

The command returns a JSON object containing:

- A map with all the backups having status ``DONE`` for that server.
- A list with all the archived WAL files.
- The configuration for the server.
- The last read position (in the xlog database file).
- The name of the last read WAL file.

The JSON response contains all the required information for the synchronisation
between the ``primary`` and a ``passive`` node.

If ``--primary`` is specified, the command is executed on the defined
primary node, rather than locally.

Configuration
-------------

Configuring a server as a passive node is a quick operation. Simply add to the server
configuration the following option:

.. code-block:: ini

	primary_ssh_command = ssh barman@primary_barman

This option specifies the SSH connection parameters to the primary server,
identifying the source of the backup data for the passive server.

If you are invoking barman with the ``-c/--config`` option and you want to use
the same option when the passive node invokes barman on the primary node then
add the following option:

.. code-block:: ini

	forward_config_path = true

Node synchronization
--------------------

When a node is marked as passive it is treated in a special way by Barman:

- It is excluded from standard maintenance operations.
- Direct operations to Postgres are forbidden, including ``barman backup``.

Synchronization between a passive server and its primary is automatically
managed by ``barman cron`` which will transparently invoke:

1. ``barman sync-info --primary``, in order to collect synchronization information.
2. ``barman sync-backup``, in order to create a local copy of every backup that is
   available on the primary node.
3. ``barman sync-wals``, in order to copy locally all the WAL files available on the
   primary node.

Manual synchronization
----------------------

Although ``barman cron`` automatically manages passive/primary node synchronization,
it is possible to manually trigger synchronization of a backup through:

.. code-block:: bash

	barman sync-backup <server_name> <backup_id>

Launching ``sync-backup`` barman will use the ``primary_ssh_command`` to connect to the
primary server, then, if the backup is present on the remote machine, it will begin to
copy all the files using rsync. Only one synchronization process per backup is allowed
at a time.

WAL files can also be synchronized, through:

.. code-block:: bash

	barman sync-wals <server_name>
