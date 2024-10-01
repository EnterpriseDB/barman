.. _quickstart:

Quick start
===========

As stated in :ref:`architectures`, we recommend setting up Barman in a dedicated host.
That said, the examples in this tutorial assume the following hosts:

* ``pghost``: The host where Postgres is running.
* ``barmanhost``: The host where Barman will be set up.

Assuming Barman is already installed in ``barmanhost`` as per
:ref:`installation <installation>`, you can continue through the next steps.

.. _quickstart-configuring-your-first-server:

Configuring your first server
-----------------------------

Barman supports different backup and WAL archive strategies. Here you can find simple
recipes to set up two of the most commonly used architectures. Choose the one that best
suits your needs and proceed to the next sections.


.. _quickstart-configuring-your-first-server-streaming-backups-with-wal-streaming:

Streaming backups with WAL streaming
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This strategy uses the Postgres streaming replication protocol for both backups and WAL
archiving, so you only need native Postgres connections between ``pghost`` and
``barmanhost`` in order to implement it. A key advantage of this approach is that no
SSH connection is required, making it a simpler option to set up in comparison with
other strategies.

It relies on the ``pg_basebackup`` utility for backups and ``pg_receivewal`` for
transferring the WAL files. It is therefore required to have both tools installed on
``barmanhost`` beforehand. Check the :ref:`Postgres client tools <pre-requisites-postgres-client-tools>`
section if you need further details on how to install them.

1. As a first step, let's create the required users you will need on your Postgres
server. On ``pghost``, execute the following commands:

.. code-block:: bash

    createuser -s -P barman

This command creates a new Postgres superuser called **barman**, which will be used by
Barman for maintenance tasks on your Postgres server.

.. code-block:: bash

    createuser -P --replication streaming_barman

This command creates a new Postgres user called **streaming_barman** with replication
privileges, which will be used by Barman when invoking ``pg_receivewal`` and
``pg_basebackup`` to transfer files to your Barman server.

Both ``createuser`` commands prompt you for a password, which you are then advised to
add to a `password file <https://www.postgresql.org/docs/current/libpq-pgpass.html>`_
named ``.pgpass`` under your Barman home directory on ``barmanhost``. Check the
:ref:`pre-requisites <pre-requisites-postgres-streaming-connection>` section if you need
further details on how to configure streaming connections.

From now on, this section assumes you already have both Postgres users created as well
as a Postgres server to be backed up. Also, we assume a database named ``postgres``
is available, so Barman can connect to the Postgres server through that database.

2. Now make sure to allow access to the previously created users from your
``barmanhost``. On ``pghost``, add these HBA rules to your ``pg_hba.conf`` file:

.. code-block:: ini

    # allows access to the barman user from barmanhost
    host    all    barman    barmanhost/32    md5
    # allows access to the streaming_barman user from barmanhost
    host    replication    streaming_barman    barmanhost/32    md5

Then, reload your Postgres configuration so the new HBA rules take effect. On
``pghost``, run:

.. code-block:: bash

    psql -c "SELECT pg_reload_conf();"


3. Still on ``pghost``, make sure your Postgres server is properly configured for
WAL streaming. On its ``postgresql.conf`` file, assert that
`wal_level <https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-WAL-LEVEL>`_
is set to ``replica`` or ``logical``:

.. code-block:: ini

    wal_level = replica

If changes were made to the ``wal_level`` configuration value, then restart your
Postgres server for the changes to take effect.

4. Now let's configure your first backup server on Barman. On ``barmanhost``, create a
file at ``/etc/barman.d/streaming-backup-server.conf`` with this content:

.. code-block:: ini

    [streaming-backup-server]
    description = "Postgres server using streaming replication"
    streaming_archiver = on
    backup_method = postgres
    streaming_conninfo = host=pghost user=streaming_barman dbname=postgres
    slot_name = barman
    create_slot = auto
    conninfo = host=pghost user=barman dbname=postgres

Where:

* ``[streaming-backup-server]`` is a name of your choice for your backup server on
  Barman.

* ``description`` is a description text for your backup server.

*  ``streaming_archiver = on`` tells Barman that WAL files of this backup server are
   transferred from Postgres to Barman using streaming replication.

* ``backup_method = postgres`` tells Barman that this server uses ``postgres`` as its
  backup method, which in essence means taking backups using ``pg_basebackup``.

* ``streaming_conninfo`` is a connection string for a :term:`libpq` connection to your
  Postgres server. This is the connection ``pg_receivewal`` and ``pg_basebackup`` use
  to transfer files to your Barman server.

* ``slot_name`` is the name of the physical replication slot in Postgres which is used
  by this backup server to stream WALs through ``pg_receivewal``.

* ``create_slot = auto`` tells Barman that it should create the replication slot
  automatically in Postgres, not requiring a manual creation beforehand.

* ``conninfo`` is a connection string for a :term:`libpq` connection to your Postgres
  server which Barman uses for maintenance purposes.

On ``barmanhost``, run:

.. code-block:: bash

    barman list-servers

You should see an output with all configured backup servers on Barman, which confirms
that it's now aware of your new server:

.. code-block:: text

    streaming-backup-server - Postgres server using streaming replication

5.  Once finished with the configuration of both Barman and Postgres servers, you
should be ready to go! Execute the following command on ``barmanhost`` to check
that everything is OK with your server:

.. code-block:: bash

    barman check streaming-backup-server

If you see failed checks related to replication slot and ``pg_receivewal``, run the
following command.

.. code-block:: bash

    barman cron

This command starts a background process that performs maintenance tasks on
your Barman servers. These tasks includes the creation of the replication slot in
Postgres, as well as the startup of ``pg_receivewal``.

Run the check command again and make sure no failed checks are shown:

.. code-block:: bash

    barman check streaming-backup-server


.. _quickstart-configuring-your-first-server-rsync-backups-with-wal-archiving:

Rsync backups with WAL archiving
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This strategy relies on Rsync and SSH connections for transferring backup and WAL
files to your Barman server.

Since it depends on SSH connections, it is therefore required that you have a
both-way passwordless SSH connection between ``pghost`` and ``barmanhost``. For
further instructions on how to set this, please refer to the
:ref:`pre-requisites <pre-requisites-ssh-connections>` section.

1. As a first step, let's create the required user you will need on your Postgres
server. On ``pghost``, execute the following command:

.. code-block:: bash

    createuser -s -P barman

This command creates a new Postgres superuser called **barman**, which will be used by
Barman for maintenance tasks as well as for issuing backup commands using the Postgres
low-level API.

The ``createuser`` command prompts you for a password, which you are then advised to
add to a `password file <https://www.postgresql.org/docs/current/libpq-pgpass.html>`_
named ``.pgpass`` under your Barman home directory on ``barmanhost``.

From now on, this section assumes you already have this Postgres user created as well
as a Postgres server to be backed up. Also, we assume a database named ``postgres``
is available, so Barman can connect to the Postgres server through that database.

2. Now make sure to allow access to the previously created user from your
``barmanhost``. On ``pghost``, add this HBA rule to your ``pg_hba.conf`` file:

.. code-block:: ini

    # allows access to the barman user from barmanhost
    host    all    barman    barmanhost/32    md5

Then, reload your Postgres configuration so the new HBA rule takes effect. On
``pghost``, run:

.. code-block:: bash

    psql -c "SELECT pg_reload_conf();"


3. Still on ``pghost``, make sure your Postgres server is properly configured for WAL
archiving. On its ``postgresql.conf`` file, assert the following parameters are
properly set:

.. code-block:: ini

    wal_level = replica
    archive_mode = on
    archive_command = 'barman-wal-archive barmanhost rsync-backup-server %p'

.. note::
    Since Barman 2.6, the recommended way of archiving WAL files via the
    ``archive_command`` is by using the ``barman-wal-archive`` utility, as in the
    example above. For this utility to be available, make sure to also have the
    ``barman-cli`` package installed on ``pghost``. Check the
    :ref:`pre-requisites-wal-archiving-via-archive-command` section for further
    details and for alternative command options.

4. Now let's configure your first backup server on Barman. On ``barmanhost``, create a
configuration file at ``/etc/barman.d/rsync-backup-server.conf`` with this content:

.. code-block:: ini

    [rsync-backup-server]
    description =  "Postgres server using Rsync and WAL archiving"
    archiver = on
    backup_method = rsync
    reuse_backup = link
    backup_options = concurrent_backup
    ssh_command = ssh postgres@pghost
    conninfo = host=pghost user=barman dbname=postgres

Where:

* ``[rsync-backup-server]`` is a name of your choice for your backup server on Barman.

* ``description`` is a description text for your backup server.

* ``archiver = on`` tells Barman that WAL files of this backup server are
  transferred from Postgres to Barman using the ``archive_command`` configured
  in Postgres.

* ``backup_method = rsync`` tells Barman that this backup server uses ``rsync`` as its
  backup method, which in essence means copying over cluster files with Rsync.

* ``reuse_backup = link`` tells Barman that you want to have data deduplication by
  reusing files of the previous backup, saving storage and network resources whenever
  taking new backups for this server. Check :ref:`rsync backups <backup-rsync-backup>`
  section for more details.

* ``backup_options = concurrent_backup`` indicates that Barman is going to issue
  non-exclusive backup commands on your Postgres server when taking backups.

* ``ssh_command`` is the SSH command to be used to connect from ``barmanhost`` to
  ``pghost``. Replace this configuration value accordingly.

* ``conninfo`` is a connection string for a :term:`libpq` connection to your Postgres
  server which Barman uses for maintenance purposes.

On ``barmanhost``, run:

.. code-block:: bash

    barman list-servers

You should see an output with all configured backup servers on Barman, which confirms
that it's now aware of your new server:

.. code-block:: text

    rsync-backup-server - Postgres server using Rsync and WAL archiving

5.  Once finished with the configuration of both Barman and Postgres servers, you
should be ready to go! Execute the following command on ``barmanhost`` to check
that everything is OK with your server:

.. code-block:: bash

    barman check rsync-backup-server

If you see a failed check related to WAL archive, don't worry. It just means that
Barman has not received any WAL files yet, probably because no WAL segment has been
switched on your Postgres server since then. You can force a WAL switch from
``barmanhost`` with this command:

.. code-block:: bash

    barman switch-wal --force rsync-backup-server

Then execute the following command, which starts a background process that performs
maintenance tasks on your Barman servers:

.. code-block:: bash

    barman cron

Run the check command again and make sure no failed checks are shown:

.. code-block:: bash

    barman check rsync-backup-server


.. _quickstart-taking-your-first-backup:

Taking your first backup
------------------------

Regardless of which strategy you chose for your backupserver, once completed with the
previous steps, you should be all set. You can run this command to take a backup:

.. code-block:: bash

    barman backup --name first-backup <server_name>

Once the command finishes, you can list all backups of your backup server with this
command:

.. code-block:: bash

    barman list-backups <server_name>

And show the details of a specific backup with this command:

.. code-block:: bash

    barman show-backup <server_name> first-backup


.. _quickstart-recovering-from-a-backup:

Recovering from a backup
------------------------

If you ever need to recover from a backup, you can do so with this command:

.. code-block:: bash

    barman recover <server_name> first-backup /path/to/recover

If recovering to a remote server, a passwordless SSH connection from the Barman host to
the destination host is required and must be specified using the
``--remote-ssh-command`` option:

.. code-block:: bash

    barman recover --remote-ssh-command="ssh user@host" <server_name> first-backup /path/to/recover
