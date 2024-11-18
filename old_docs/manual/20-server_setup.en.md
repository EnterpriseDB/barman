\newpage

# Setup of a new server in Barman

As mentioned in the _"Design and architecture"_ section, we will use the
following conventions:

- `pg` as server ID and host name where PostgreSQL is installed
- `backup` as host name where Barman is located
- `barman` as the user running Barman on the `backup` server (identified by
the parameter `barman_user` in the configuration)
- `postgres` as the user running PostgreSQL on the `pg` server

> **IMPORTANT:** a server in Barman must refer to the same PostgreSQL
> instance for the whole backup and recoverability history (i.e. the
> same system identifier). **This means that if you perform an upgrade
> of the instance (using for example `pg_upgrade`, you must not reuse
> the same server definition in Barman, rather use another one as they
> have nothing in common.**
