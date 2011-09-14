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

2ndQuadrant has always paid attention through his founder's contribution to
disaster recovery, by heavily contributing to PostgreSQL Point-In-Time-Recovery.
In 2011, in order to simplify the management of PostgreSQL backups,
2ndQuadrant started the development of *BaRMan*, physical Backup and Recovery Manager.

== Before you start

TODO: Decide backup architecture. Plan your backup policy and workflow.

=== System requirements

TODO:
* Python 2.4 or higher (recommended: with setuptool or distribute modules)
* PsycoPG 2
* PostgreSQL >= 8.4
* rsync >= 3.0.4

=== Installation

=== Using the sources

TODO

=== On Debian/Ubuntu

TODO

=== On RedHat/CentOS/SL

TODO

== Getting started

=== Basic configuration

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
