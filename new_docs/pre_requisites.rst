.. _pre-requisites:

Pre-requisites
==============

This section details some requirements and configurations necessary to set up a Barman
environment depending on your use case.

Throughout this section, we assume the following hosts:

* ``pghost``: The host where Postgres is running.

* ``barmanhost``: The host where Barman will be set up.


.. _pre-requisites-postgres-connection:

Postgres connection
-------------------

A connection to your Postgres instance is required regardless of which backup method
you are using. This connection is required by Barman in order to coordinate its
activities with the database server, as well as for monitoring purposes.

.. note::

    The ``createuser`` commands executed below prompt you for a password, which you
    are then advised to add to a
    `password file <https://www.postgresql.org/docs/current/libpq-pgpass.html>`_
    named ``.pgpass`` under your Barman home directory on ``barmanhost``. Aditionally,
    you can choose the client authentication method of your preference among those
    offered by Postgres. Check the `official documentation <https://www.postgresql.org/docs/current/client-authentication.html>`_
    for further details.

Make sure that ``barmanhost`` can connect to the database server as superuser or with
a user with the required priviledges.

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


.. _pre-requisites-postgres-streaming-connection:

Postgres streaming replication connection
-----------------------------------------

If you plan to use streaming backups, you need to setup a streaming connection.
Additionally, if you plan on streaming WAL files you will need to also install the
Postgres client package which provides the ``pg_receivewal`` tool.

To install the Postgres client package on Debian or Ubuntu run the following command
on the ``barmanhost``:

.. code-block:: bash

    sudo apt-get install postgresql-client

Alternatively, if the ``barmanhost`` is using RHEL, Rocky Linux, Alma Linux, follow
this recipe:

.. code-block:: bash

    sudo dnf install postgresql

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

