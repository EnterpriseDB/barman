.. _pre-requisites:

Pre-requisites
==============

This section details some requirements and configurations necessary to set up a Barman
environment depending on your use case.

Throughout this section, we assume the following hosts:

* ``pghost``: The host where Postgres is running.

* ``barmanhost``: The host where Barman will be set up.

.. _pre-requisites-postgres-user:

Postgres users
--------------

Barman requires a connection to your Postgres instance to gather information about the
server. The recommended way to set up this connection is to create a dedicated user in
Postgres named ``barman``. This user should have the necessary privileges.

.. note::

    The ``createuser`` commands executed below will prompt you for a password, which
    you are then advised to add to a
    `password file <https://www.postgresql.org/docs/current/libpq-pgpass.html>`_
    named ``.pgpass`` under your Barman home directory on ``barmanhost``. Aditionally,
    you can choose the client authentication method of your preference among those
    offered by Postgres. Check the `official documentation <https://www.postgresql.org/docs/current/client-authentication.html>`_
    for further details.


To create a superuser named ``barman`` in Postgres, run the following command on
``pghost``:

.. code-block:: bash

    createuser -s -P barman

Or, in case you opt for a user with only the required priviledges, follow these steps:

1. On ``pghost``, run this command to create a user named ``barman`` in Postgres:

.. code-block:: bash

    createuser -P barman

2. On ``pghost``, in the psql interface, run the following statements:

.. code-block:: sql

    GRANT EXECUTE ON FUNCTION pg_backup_start(text, boolean) to barman;
    GRANT EXECUTE ON FUNCTION pg_backup_stop(boolean) to barman;
    GRANT EXECUTE ON FUNCTION pg_switch_wal() to barman;
    GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) to barman;
    GRANT pg_read_all_settings TO barman;
    GRANT pg_read_all_stats TO barman;

In the case of using Postgres version 14 or a prior version, the functions
``pg_backup_start`` and ``pg_backup_stop`` will have different names and signatures.
You will therefore need to replace the first two lines in the above block with:

.. code-block:: sql

    GRANT EXECUTE ON FUNCTION pg_start_backup(text, boolean, boolean) to barman;
    GRANT EXECUTE ON FUNCTION pg_stop_backup() to barman;
    GRANT EXECUTE ON FUNCTION pg_stop_backup(boolean, boolean) to barman;

.. note::

    In Postgres version 13 and below, the ``--force`` option of the barman
    ``switch-wal`` command does not work without a superuser. In Postgres version 15 or
    above, it is possible to grant the ``pg_checkpoint`` role to use it without a
    superuser by executing the statement ``GRANT pg_checkpoint TO barman;``.


.. _pre-requisites-postgres-connection:

Postgres connection
-------------------

A connection to your Postgres instance is required regardless of which backup method
you are using. This connection is required by Barman in order to coordinate its
activities with the database server, as well as for monitoring purposes.

Make sure that ``barmanhost`` can connect to the database server as superuser or with
a user with the required priviledges. You can find detailed information about
setting up Postgres connections in the
`Postgres Client Authentication <https://www.postgresql.org/docs/current/client-authentication.html>`_.

With your user created, run the following command on ``barmanhost`` to assert that it
can connect to your Postgres instance:

.. code-block:: bash

    psql -c 'SELECT version()' -U barman -h pghost postgres


``postgres`` can be any available database through which Barman can connect to the
Postgres instance.

If the above command succeeds, it means that it can successfully connect to the
database server. Remember the connection parameters above as they are the ones you
need to write on your server's configuration file, in the ``conninfo`` parameter.
In this context, this parameter would be as follows:


.. code-block:: ini

    [my-server]
    ; ...
    conninfo = host=pghost user=barman dbname=postgres application_name=myapp

``application_name`` is an optional parameter.

.. _pre-requisites-postgres-client-tools:

Postgres client tools
---------------------

The Postgres client tools are required to interact with the Postgres server. The most
commonly used tools by Barman are ``pg_basebackup`` and ``pg_receivewal``. They are
provided by the Postgres client package.

To install the Postgres client package on Debian or Ubuntu run the following command
on the ``barmanhost``:

.. code-block:: bash

    sudo apt-get install postgresql-client

Alternatively, if the ``barmanhost`` is using RHEL, Rocky Linux, Alma Linux, follow
this recipe:

.. code-block:: bash

    sudo dnf install postgresql


.. _pre-requisites-postgres-streaming-connection:

Postgres streaming replication connection
-----------------------------------------

If you plan to use streaming backups or streaming of WAL files, you need to
setup a streaming connection. Additionally, you also need to have the Postgres
client tools installed, as shared in
:ref:`pre-requisites <pre-requisites-postgres-client-tools>` section.

We recommend creating a dedicated user in Postgres named ``streaming_barman``. You
can do so with the following command:

.. code-block:: bash

    createuser -P --replication streaming_barman


.. note::

    The ``createuser`` commands executed below prompt you for a password, which you
    are then advised to add to a
    `password file <https://www.postgresql.org/docs/current/libpq-pgpass.html>`_
    named ``.pgpass`` under your Barman home directory on ``barmanhost``. Aditionally,
    you can choose the client authentication method of your preference among those
    offered by Postgres. Check the `official documentation <https://www.postgresql.org/docs/current/client-authentication.html>`_
    for further details.

You can verify that the streaming connection works through the following command:

.. code-block:: bash

    psql -U streaming_barman -h pghost -c "IDENTIFY_SYSTEM" replication=1

If the connection is working, you should see a response containing the system
identifier, current timeline ID and current WAL flush location, for example:

.. code-block:: text

          systemid       | timeline |  xlogpos   | dbname
    ---------------------+----------+------------+--------
    7139870358166741016 |        1 | 1/330000D8 |
    (1 row)

You also need to configure the ``max_wal_senders`` parameter in Postgres.
The number of WAL senders depends on the Postgres architecture you have implemented.
In this example, we are setting it to ``2``:

.. code-block:: ini

    max_wal_senders = 2

This option represents the maximum number of concurrent streaming connections that
Postgres is allowed to manage.

Another important parameter is ``max_replication_slots``, which represents the maximum
number of replication slots that Postgres is allowed to manage. This parameter is
relevant if you are planning to use the streaming connection to receive WAL files over
the streaming connection:

.. code-block:: ini

    max_replication_slots = 2

The values proposed for ``max_replication_slots`` and ``max_wal_senders`` must be
considered as examples, and the values you use in your actual setup must be chosen
after a careful evaluation of the architecture. Please consult the Postgres
documentation for guidelines and clarifications.


.. _pre-requisites-ssh-connections:

SSH connections 
---------------

If you plan to use Rsync backups or WAL archiving via ``archive_command``, then SSH
connections are required.

SSH is a protocol and a set of tools that allows you to open a remote shell to a remote
server and copy files between the server and the local system. You can find more
documentation about SSH usage in the `article "SSH Essentials" by Digital Ocean <https://www.digitalocean.com/community/tutorials/ssh-essentials-working-with-ssh-servers-clients-and-keys>`_.

SSH key exchange is a very common practice that is used to implement secure
passwordless connections between users on different machines, and it's needed to use
Rsync for WAL archiving and backups.


.. _pre-requisites-ssh-connections-ssh-configuration-of-postgres-user:

SSH configuration of postgres user
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Unless you have done it before, you need to create an SSH key for the **postgres**
user. Log in as **postgres**  on ``pghost`` and run:

.. code-block:: bash

    ssh-keygen -t rsa

As this key must be used to connect from hosts without providing a password, no
passphrase should be entered during the key pair creation.


.. _pre-requisites-ssh-connections-ssh-configuration-of-barman-user:

SSH configuration of barman user
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You also need to create an SSH key for the **barman** user. Log in as **barman** on
``barmanhost``  and run:

.. code-block:: bash

    ssh-keygen -t rsa

Again, no passphrase should be entered.


.. _pre-requisites-ssh-connections-from-postgres-to-barman:

From Postgres to Barman
^^^^^^^^^^^^^^^^^^^^^^^

The SSH connection from ``pghost``  to ``barmanhost`` is needed to correctly archive
WAL files using the ``archive_command``.

To successfully connect from ``pghost`` to ``barmanhost``, the **postgres** user`s
public key has to be stored in the authorized keys of the **barman** user on
``barmanhost``. This key is located in the **postgres** user home director in a file
named ``.ssh/id_rsa.pub``, and its content should be included in a file named
``.ssh/authorized_keys`` inside the home directory of the **barman** user on
``barmanhost``. If the ``authorized_keys`` file doesn't exist, create it using
``600`` as permissions.

The following command should succeed without any output if the SSH key pair exchange
has been completed successfully:

.. code-block:: bash

    ssh barman@barmanhost -C true


.. _pre-requisites-ssh-connections-from-barman-to-postgres:

From Barman to Postgres
^^^^^^^^^^^^^^^^^^^^^^^

The SSH connection between from ``barmanhost`` to ``pghost``  is used for the
traditional backup using Rsync.

To successfully connect from ``barmanhost`` to ``pghost``, the **barman** user`s
public key has to be stored in the authorized keys of the **postgres** user on
``pghost``. This key is located in the **barman** user home directory in a file
named ``.ssh/id_rsa.pub``, and its content should be included in a file named
``.ssh/authorized_keys`` inside the home directory of the **postgres** user on
``pghost``. If the ``authorized_keys`` file doesn't exist, create it using
``600`` as permissions.

The following command should succeed without any output if the SSH key pair exchange
has been completed successfully:

.. code-block:: bash

    ssh postgres@pghost -C true

.. _pre-requisites-wal-archiving-via-archive-command:

WAL archiving via ``archive_command``
-------------------------------------

As stated in the :ref:`architectures-wal-archiving-strategies` section, there are two
options to archive wals with Barman. If you wish to use the streaming replication
protocol to archive WAL files, refer to the :ref:`concepts-barman-concepts-wal-streaming`
concepts and :ref:`quickstart` section, specifically the Streaming backups with WAL
streaming sub-section. Otherwise you can configure WAL archiving using the
``archive_command`` with :ref:`commands-barman-cli-barman-wal-archive` or with
Rsync/SSH.

Using barman-wal-archive
^^^^^^^^^^^^^^^^^^^^^^^^

Starting from Barman 2.6, the recommended approach for securely archiving Write-Ahead
Log files is to utilize the ``barman-wal-archive`` command from the ``barman-cli``
package. Refer to the :ref:`installation <installation>` section on how to install this
package.

Using ``barman-wal-archive`` instead of traditional methods like rsync or SSH minimizes
the risk of data corruption during the transfer of WAL files to the Barman server. The
conventional methods lack a guarantee that the file's content is properly flushed and 
fsynced to disk at the destination.

The ``barman-wal-archive`` utility directly interacts with
:ref:`commands-barman-put-wal` command. This command ensures that the received WAL file
is fsynced and stored in the correct incoming directory for the respective server. The
only parameter required for the ``archive_command`` is the server's name, reducing the
likelihood of misplacement.

To verify that ``barman-wal-archive`` can connect to the Barman server and that the
Postgres server is correctly configured to accept incoming WAL files, execute the
following command:

.. code-block:: text

    barman-wal-archive --test backup pg DUMMY

Here, ``backup`` refers to the Barman host, ``pg`` is the Postgres server's name as
configured in Barman, and ``DUMMY`` is a placeholder for the WAL file name which is
ignored when using the ``-t`` option.

If the setup is correct, you should see:

.. code-block:: text

    Ready to accept WAL files for the server pg

Since the utility communicates via SSH, ensure that SSH key authentication is set up for
the postgres user to log in as barman on the backup server. If your SSH connection uses
a port other than the default (22), you can specify the port using the ``--port``
option.

Refer to the
:ref:`quickstart-configuring-your-first-server-rsync-backups-with-wal-archiving` to start
working with it.

Using Rsync/SSH
^^^^^^^^^^^^^^^

An **alternative approach** for configuring the ``archive_command`` is to utilize the
rsync command via SSH. Here are the initial steps to set it up effectively for a
Postgres server named ``pg``, a Barman server named ``backup`` and a user named
``barman``.

To locate the incoming WALs directory, use the following command and check for the
``incoming_wals_directory`` value:

.. code-block:: text

    barman show-servers pg | grep incoming_wals_directory
    
        incoming_wals_directory: /var/lib/barman/pg/incoming

Next, edit the ``postgresql.conf`` file for the Postgres instance on the ``pg`` host to
enable archive mode:

.. code-block:: text

    archive_mode = on
    wal_level = 'replica'
    archive_command = 'rsync -a %p barman@backup:INCOMING_WALS_DIRECTORY/%f'

Be sure to replace the ``INCOMING_WALS_DIRECTORY`` placeholder with the actual path
retrieved from the previous command. After making these changes, restart the Postgres
server.

For added security in the ``archive_command`` process, consider implementing stricter
checks. For instance, the following command ensures that the hostname matches before
executing the rsync:

.. code-block:: text

    archive_command = 'test $(/bin/hostname --fqdn) = HOSTNAME \
        && rsync -a %p barman@backup:INCOMING_WALS_DIRECTORY/%f'

Replace ``HOSTNAME`` with the output from ``hostname --fqdn``. This approach acts as a
safeguard against potential issues when servers are cloned, preventing WAL files from
being sent by recovered Postgres instances.
