.. _warehousepg:

WarehousePG
===========

`WarehousePG <https://www.warehousepg.com>`_ (WHPG) is a PostgreSQL-based
massively parallel processing database. A WarehousePG cluster is made of
several PostgreSQL instances: the coordinator and the segments, which can
share a host.

Barman is not a disaster recovery tool for WarehousePG clusters as a whole. It
takes physical backups of, and restores, the **individual instances** which
compose a cluster. Orchestrating those operations across the cluster is
the job of an external tool, such as ``whpg-dr``.

This page covers what Barman needs in order to work with a WarehousePG
instance.

Connecting to an instance
-------------------------

Each WarehousePG instance is configured in Barman as an independent server,
with its own ``conninfo``. Segments only accept direct connections in *utility
mode*, so the connection parameters have to enable it:

* WarehousePG 7: ``gp_role=utility``
* WarehousePG 6: ``gp_session_role=utility``

As an example, for a WarehousePG 7 segment:

.. code-block:: ini

    [seg1]
    description = "WarehousePG 7 segment"
    backup_method = postgres
    conninfo = host=seg1 port=6000 user=barman dbname=postgres options='-cgp_role=utility'

How Barman detects a WarehousePG instance
-----------------------------------------

Barman considers an instance to be a WarehousePG one when both the
``gp_dbid`` and ``gp_server_version_num`` settings exist in the instance it is
connected to through ``conninfo``. The ``version()`` string is not used for
that.

If only one of the two settings exists, the instance is in an unexpected state
and Barman fails the operation instead of making assumptions about it.

When taking a backup of a WarehousePG instance, Barman saves the following
information in the backup metadata, and shows it in ``barman show-backup``:

* The instance identifier in the cluster, i.e. the value of ``gp_dbid``.
* The WarehousePG version number, i.e. the value of ``gp_server_version_num``,
  e.g. ``70000`` for WarehousePG 7.0.

Backups of other PostgreSQL instances do not have this information.

.. note::
    Backups taken before Barman 3.21 are not recognized as WarehousePG
    backups.
