\newpage

# Setup of a new server in Barman

As mentioned in the _"Design and architecture"_ section, we will use the
following conventions:

- `pg` as server ID and host name where PostgreSQL is installed
- `backup` as host name where Barman is located
- `barman` as the user running Barman on the `backup` server (identified by
the parameter `barman_user` in the configuration)
- `postgres` as the user running PostgreSQL on the `pg` server
