= BaRman

Backup and Recovery Manager for PostgreSQL

== Introduction

In a perfect world, there would not be the need for a backup.
However, the unexpected is always upon us. And it is important,
especially in business environments, to be prepared for when the "unexpected" happens.
In a database scenario, the "unexpected" could be any of the following:

* data corruption
* system failure, including hardware failures
* human errors

In these cases, any ICT manager or DBA should be able to repair from
the incident and recover in the shortest time possible.
We normally refer to this discipline as *Disaster recovery*.

This guide assumes you are familiar with theoretical disaster recovery concepts
and you have a grasp of PostgreSQL fundamentals in terms of physical backup and
disaster recovery. If not, we encourage you to read the PostgreSQL documentation
or any of the recommended books on PostgreSQL.

Professional training on this topic is another effective
way of learning these concepts. There are many courses available all year round all over the world,
delivered by many PostgreSQL companies, including our company, 2ndQuadrant.

For now, it is important to know that any PostgreSQL physical backup will be made up of:

* a base backup
* one or more WAL files (usually collected through continuous archiving)

PostgreSQL offers the core primitives in order to allow DBAs to setup a really robust
Disaster Recovery environment. However, it is not that easy to manage multiple backups,
from one or more PostgreSQL servers. Restore is another topic that any PostgreSQL DBA
would love to see more automated and user friendly. Other commercial vendor have this
kind of applications.

With these goals in mind, 2ndQuadrant started the development of BaRMan, which stands for
"Backup and Recovery Manager" for PostgreSQL. Currently BaRMan works on Linux and Unix
systems only.

== Before you start

The first step is to decide the architecture of your backup. In a simple scenario, you have
one *PostgreSQL instance* (server) running on a host. You want your data continuously backed up
on another server, called the *backup server*.
BaRMan allows you to launch PostgreSQL backups directly from the backup server, using SSH
connections.
Another key feature of BaRMan is that it allows you to centralise your backups in case
you have more than one PostgreSQL servers to manage.
During this guide, we will assume that you have one PostgreSQL instance on an host
and one backup server on another host. It is important to note that for disaster recovery,
these two servers should not share any physical resources but the network. You can use
BaRMan in geographical redundancy scenarios for better disaster recovery outcomes.

TODO: Plan your backup policy and workflow (with version 0.3).

=== System requirements

TODO:
* Linux/Unix (what about Windows?)
* Python 2.4 or higher (recommended: with setuptool or distribute modules)
* PsycoPG 2
* PostgreSQL >= 8.4
* rsync >= 3.0.4

=== Installation

=== Using the sources

As @postgres@ user:

./setup.py install --user

=== On Debian/Ubuntu

TODO

=== On RedHat/CentOS/SL

TODO

== Getting started

=== Pre-Requisites

TODO: SSH Key exchange and WAL archiving

=== Basic configuration

TODO

=== Listing the servers

TODO

=== Executing a full backup

TODO

=== Restoring a whole server

TODO

=== Restoring to a point in time

TODO

== Available commands

Barman allows you to specify commands at three different stages:

* global: commands on the local backup catalog
* server: commands for a specific server (list available backups, execute a backup, etc.)
* specific backup: commands for a specific backup in the catalog (display information or issue a recovery, delete the backup, etc.)

The following sections will thoroughly describe the available commands, section per section.

=== General commands

=== Server commands

=== Backup commands

== Advanced configuration

== Support and sponsor opportunities

Barman is free software and it is written and maintained by 2ndQuadrant.
If you need support on Barman or need new features, please get in touch with 2ndQuadrant.
You can sponsor the development of new features of Barman and PostgreSQL which will be made publicly available as open source.

== Authors

2ndQuadrant website: http://www.2ndquadrant.it/

* Marco Nenciarini <marco.nenciarini@2ndquadrant.it>
* Gabriele Bartolini <gabriele.bartolini@2ndquadrant.it>

== License

Barman is the property of 2ndQuadrant and its code is distributed under GNU General Public License 3.
