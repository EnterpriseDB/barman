.. _retention_policies:

Retention policies
==================

Overview
--------

Minimum redundancy safety
-------------------------

You can set a minimum number of backups for your PostgreSQL server using the
``minimum_redundancy`` option in the global or per-server configuration. By default, this
option is set to 0.

If you set ``minimum_redundancy`` to a number greater than 0, Barman will ensure that you
always have at least that many backups available on the server.

This setting helps protecting against accidental deletion of backups.

.. note:: 
    Make sure your retention policy does not conflict with the minimum redundancy
    setting. Check Barman's logs regularly for any related messages.

Scope of retention policies
---------------------------
Barman allows you to define retention policies by two methods.

Backup Redundancy
"""""""""""""""""

Specifies the number of backups to retain. Barman keeps the most recent backups up to the
specified number. This type of policy does not consider the time period for retention but
focuses on the number of backups.

For example, if you set a redundancy of 3, Barman will retain the three most recent
backups and discard older ones.

Recovery Window
"""""""""""""""

Specifies the duration for which backups must be retained to allow recovery to any point
within that window. The interval window always ends at the current time and spans
backward for the specified period. Barman retains backups and archive logs necessary for
point-in-time recovery to any moment within this window.

For example, if you set a 7-day recovery window, Barman will keep backups and archive
logs to allow recovery to any point within the past 7 days. This means that the first
backup that falls outside the window will still be retained with its corresponding WALs,
but backups before this one and all the older WALs will be marked as obsolete and
eventually evicted.

Use cases
---------

Point-In-Time Recovery
""""""""""""""""""""""

Base backups and archive logs have the same retention policy. This setup allows you to
recover your PostgreSQL server to any point in time from the end time of the earliest
available backup.

Operational Efficiency and Space Management
"""""""""""""""""""""""""""""""""""""""""""

You may want to maintain a certain number of recent backups while periodically removing
older ones to save on storage cost and manage storage space effectively, especially in
environments with limited resources.

Long-Term Archival
""""""""""""""""""

For compliance or historical purposes, you may need to retain backups for extended
periods beyond the usual operational requirements. This is often required in regulated
industries where data must be kept for a certain period.

How retention policies are enforced
-----------------------------------

Retention policies in Barman are enforced automatically by Barman's maintenance tasks
which are executed by ``barman cron``.

Configurations and Syntax
-------------------------

Retention policies are configured globally or per server using the ``retention_policy``
option offering flexibility in a multi-server environment. By default, the value of the
``retention_policy`` option is not set, so no retention is enforced.

Retention policies have the following syntax:

``retention_policy = {REDUNDANCY value | RECOVERY WINDOW OF value {DAYS | WEEKS | MONTHS}}``

* value must be an integer greater than 0.
* For backup redundancy, value must meet or exceed the server's minimum redundancy
  level.
* For recovery window, value must be at least as high as the server's minimum
  redundancy level in reverse order.
* If value is not assigned, a warning is generated.

.. important::
    Block-level incremental backups are not considered in retention policies, as they
    depend on their parent backups and the root backup. Only the root backup is used
    to determine retention.

Retention policy for block-level incremental backups
----------------------------------------------------

When retention policy is applied:

* Barman will focus on the root backup.
* If the root backup is marked as ``KEEP:FULL``, ``KEEP:STANDALONE`` or ``VALID``, all
  associated incremental backups are marked as ``VALID``.
* If the root backup is marked as ``OBSOLETE``, all associated incremental backups are
  marked as ``OBSOLETE``.