.. _concepts:

Concepts
========

Creating a disaster recovery plan can be challenging, especially for those unfamiliar
with the various backup concepts involved in backup management. There are many
different methods for taking backups, each with its own advantages, disadvantages, and
technical requirements. The choice of the right approach will depend on your resources,
environment and technical knowledge. Knowing that not everyone might be well-grounded
in this context, this section is dedicated to explaining the most fundamental concepts
regarding database backups, particularly in the context of Postgres and Barman.


.. _concepts-introduction:

Introduction
------------

In a perfect world, there would be no need for backups. However, it is important,
especially in critical business environments, to be prepared for when the unexpected
happens. In a database scenario, the "unexpected" could take any of the following
forms:

* Data corruption;
* System failure (including hardware failure);
* Human error;
* Natural disaster.

In such cases, any :term:`ICT` manager or :term:`DBA` should be able to fix the
incident and recover the database in the shortest time possible. We normally refer to
this discipline as disaster recovery, and more broadly as business continuity.

Within business continuity, it is important to familiarize yourself with two
fundamental metrics, as defined by Wikipedia:

* Recovery Point Objective (RPO): the maximum targeted period in which data might be
  lost from an IT service due to a major incident.
* Recovery Time Objective (RTO): the targeted duration of time and a service level
  within which a business process must be restored after a disaster (or disruption) in
  order to avoid unacceptable consequences associated with a breakage in business
  continuity.

In a few words, RPO represents the maximum amount of data you can afford to lose, while
RTO represents the maximum down-time you can afford for your service.

Understandably, we all want RPO=0 (zero data loss) and RTO=0 (zero down-time, utopia) -
even if it is our grandmother's recipe website. In reality, a careful cost analysis
phase is required to determine your business continuity requirements.

Fortunately, with an open source stack composed of Barman and Postgres, you can achieve
RPO=0 thanks to synchronous streaming replication. RTO is more the focus of a High
Availability solution, like ``Patroni`` or ``repmgr``. Therefore, by integrating Barman any 
of these tools, you can dramatically reduce RTO to nearly zero. 

In any case, it is important for us to emphasize more on cultural aspects related to
disaster recovery, rather than the actual tools. Tools without human beings are
useless. Our mission with Barman is to promote a culture of disaster recovery that:

* Focuses on backup procedures;
* Focuses even more on recovery procedures;
* Relies on education and training on strong theoretical and practical concepts of
  Postgres crash recovery, backup, Point-In-Time-Recovery, and replication for your
  team members;
* Promotes testing your backups (only a backup that is tested can be considered to be
  valid), either manually or automatically (be creative with Barman's hook scripts!);
* Fosters regular practice of recovery procedures, by all members of your devops team
  (yes, developers too, not just system administrators and :term:`DBAs <DBA>`);
* Solicits regularly scheduled drills and disaster recovery simulations with the
  team every 3-6 months;
* Relies on continuous monitoring of Postgres and Barman, and that is able to promptly
  identify any anomalies.

Moreover, do everything you can to prepare yourself and your team for when the disaster
happens (yes, when), because when it happens:

* It is going to be a Friday evening, most likely right when you are about to leave the
  office.
* It is going to be when you are on holiday (right in the middle of your cruise around
  the world) and somebody else has to deal with it.
* It is certainly going to be stressful.
* You will regret not being sure that the last available backup is valid.
* Unless you know how long it approximately takes to recover, every second will seem
  like forever.

In 2011, with these goals in mind, 2ndQuadrant started the development of Barman, now
one of the most used backup tools for Postgres. Barman is an acronym for "Backup and
Recovery Manager".

Be prepared, don't be scared.


General backup concepts
-----------------------

Postgres backup concepts and terminology
----------------------------------------

Barman concepts and terminology
-------------------------------

Outstanding features from Barman
--------------------------------
