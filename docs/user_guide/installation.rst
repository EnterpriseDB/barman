.. _installation:

Installing
==========

Barman official packages are provided by :term:`PGDG`. These packages use the default
version of Python 3 that comes with the operating system.

There are three packages that make up the suite of Barman features: ``barman``,
``barman-cli`` and ``barman-cli-cloud``.

* ``barman`` is the main package and it must be installed.

* ``barman-cli`` is an optional package that holds the ``barman-wal-restore`` and
  ``barman-wal-archive`` utilites. This package is mandatory if you plan to use those
  utilities as the ``archive_command`` or ``restore_command``. It must be installed on
  each Postgres server that is part of the Barman cluster.

* ``barman-cli-cloud`` is an optional package that holds the ``barman-cloud-*`` client
  scripts that you can use to manage backups in a cloud provider. It must be installed
  on the Postgres servers that you want to back up directly to a cloud provider,
  bypassing Barman.


.. note::
    Barman packages can be found in several different repositories. We recommend using
    PGDG repositories because it ensures compatibility, stability and access to
    the latest updates.

.. warning::
    Do not upgrade Barman using different repositories. By doing so you risk losing your
    configuration as each source repository provides different packages, which use
    different configuration layouts.

.. _installation-system-requirements:

System requirements
-------------------

The minimal system requirements needed to run a Barman server are the following:

* Linux operating system (Debian, Ubuntu, RHEL, Rocky, Fedora, etc.) or UNIX-like
  operating system (FreeBSD, OpenBSD, etc.)
* Python 3.6 or higher
* Python modules:

  * ``psycopg2`` >= 2.4.2: Required to connect to the Postgres server
  * ``python-dateutil``
  * ``setuptools``
  * ``argcomplete`` (optional)
* PostgreSQL client tools: Required to interact with the Postgres server
* PostgreSQL server >= 13
* ``rsync`` >= 3.1.0: Required for recovery and Rsync backups
* ``boto3`` >= 1.29.1: Required when using ``backup_method = snapshot`` together with
  the snapshot lock feature on AWS
* ``file`` POSIX command, generally provided by the ``file`` package

.. note::
   Users of RedHat Enterprise Linux, RockyLinux and AlmaLinux are required to install
   the `Extra Packages Enterprise Linux (EPEL) repository <https://fedoraproject.org/wiki/EPEL>`

.. _installation-rhel-based-distributions:

RHEL-based distributions
------------------------

You can install ``barman``, ``barman-cli`` and ``barman-cli-cloud`` using :term:`RPM`
packages on :term:`RHEL` systems as well as on similar RHEL-based systems like
AlmaLinux, Oracle Linux and Rocky Linux.

To begin with the installation, first install the
`PGDG RPM repository <https://www.postgresql.org/download/linux/redhat/>`_.

.. important::
   The ``barman-cli-cloud`` scripts are part of the ``barman-cli`` package for
   RHEL-based distributions from :term:`PGDG`. Therefore, you only need to install
   ``barman-cli`` to use the cloud scripts.

barman
^^^^^^

To install the ``barman`` package. Run as **root**:

.. code-block:: bash

   dnf install barman

barman-cli
^^^^^^^^^^

To install the ``barman-cli`` package, run as **root** in the Postgres server:

.. code-block:: bash

   dnf install barman-cli

.. note::
   If you want to use the barman-cloud utilities as
   :ref:`hook scripts <hook-scripts-using-barman-cloud-scripts-as-hooks-in-barman>`, you
   will need to install the ``barman-cli`` package in the Barman server.

.. _installation-debian-based-distributions:

Debian-based distributions
--------------------------

You can install ``barman``, ``barman-cli`` and ``barman-cli-cloud`` using :term:`DEB` packages
on Debian systems as well as on Debian-based systems like Ubuntu.

To begin with the installation, install the PGDG APT repository. This depends on your system:

* For Debian: `PGDG Debian repository <https://www.postgresql.org/download/linux/debian/>`_.
* For Ubuntu: `PGDG Ubuntu repository <https://www.postgresql.org/download/linux/ubuntu/>`_.

.. important::
   The ``barman-cli-cloud`` package is included among the recommended packages when you
   install ``barman-cli``.
   
   Before starting the installation, it's essential to evaluate your use case. If you
   don't plan to use the barman-cloud client scripts, such as ``barman-cloud-backup``,
   you can skip installing ``barman-cli-cloud`` as a recommended package when
   installing ``barman-cli``. However, if you only intend to use the barman-cloud client
   scripts, you can install the ``barman-cli-cloud`` package on its own.

barman
^^^^^^

To install the ``barman`` package. Run as **root**:

.. code-block:: bash

   apt install barman

barman-cli
^^^^^^^^^^

To install the ``barman-cli`` package, run as **root** in the Postgres server:

.. code-block:: bash

   apt install barman-cli

barman-cli-cloud
^^^^^^^^^^^^^^^^

To install the ``barman-cli-cloud`` package, run as **root** in the Postgres server:

.. code-block:: bash

   apt install barman-cli-cloud

.. note::
   If you want to use the barman-cloud utilities as
   :ref:`hook scripts <hook-scripts-using-barman-cloud-scripts-as-hooks-in-barman>`, you
   will need to install this package in the Barman server.

.. _installation-sles-based-distributions:

SLES-based distributions
------------------------

You can install ``barman`` on :term:`SLES` systems by utilizing the packages provided in
the `PostgreSQL Zypper Repository <https://zypp.postgresql.org/>`_.

To begin installation, you will need to add the appropriate repository by following the
detailed instructions available on the
`PGDG SLES Repository Configuration <https://zypp.postgresql.org/howtozypp/>`_.

**The current supported version for installation is SLES 15 SP6.**

.. important::
   The ``barman-cli-cloud`` utilities are part of the ``barman-cli`` package for
   SLES-based distributions from :term:`PGDG`. Therefore, you only need to install
   ``barman-cli`` to use the cloud scripts.

barman
^^^^^^

To install the ``barman`` package. Run as **root**:

.. code-block:: bash

   zypper install barman

barman-cli
^^^^^^^^^^

To install the ``barman-cli`` package, run as **root** in the Postgres server:

.. code-block:: bash

   zypper install barman-cli

.. note::
   If you want to use the barman-cloud utilities as
   :ref:`hook scripts <hook-scripts-using-barman-cloud-scripts-as-hooks-in-barman>`, you
   will need to install this package in the Barman server.