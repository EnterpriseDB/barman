.. _installation:

Installing
==========

Barman official packages are provided by :term:`PGDG`.
These packages use the default version of Python 3 that comes with the operating system.

.. note::
    Barman packages can be found in several different repositories. We recommend using
    PGDG repositories because it ensures compatibility, stability and access to
    the latest updates.

.. warning::
    Do not upgrade Barman using different repositories. By doing so you risk losing your
    configuration as each source repository provides different packages, which use
    different configuration layouts.

RHEL-based distributions
------------------------

Barman can be installed using :term:`RPM` packages on :term:`RHEL` systems as well as on
similar RHEL-based systems like AlmaLinux, Oracle Linux and Rocky Linux.

To install Barman:

1. Install the `PGDG RPM repository <https://www.postgresql.org/download/linux/redhat/>`_.
2. Install the Barman package. Run as **root**:

.. code-block:: bash

   dnf install barman

Debian-based distributions
--------------------------

Barman can be installed using :term:`DEB` packages on Debian systems as well as on
Debian-based systems like Ubuntu.

To install Barman:

1. Install the PGDG APT repository. This depends on your system:

   * For Debian: `PGDG Debian repository <https://www.postgresql.org/download/linux/debian/>`_.
   * For Ubuntu: `PGDG Ubuntu repository <https://www.postgresql.org/download/linux/ubuntu/>`_.

2. Install the Barman package. Run as **root**:

.. code-block:: bash

   apt-get install barman
