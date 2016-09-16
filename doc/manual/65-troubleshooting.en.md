\newpage

# Troubleshooting

## Diagnose a Barman installation

You can gather important information about the status of all
the configured servers using:

``` bash
barman diagnose
```

The `diagnose` command output is a full snapshot of the barman server,
providing useful information, such as global configuration, SSH version,
Python version, `rsync` version, PostgreSQL clients version,
as well as current configuration and status of all servers.

The diagnose command is extremelly usefull for troubleshooting problems and
having a global view of the barman status.

## Requesting for help

Although Barman is extensively documented there are a lot of scenarios that
are not covered.

For any question around Barman and disaster recovery scenarios using Barman,
you can reach the dev team using the official mailing list:
https://groups.google.com/group/pgbarman

or the IRC channel on freenode:
irc://irc.freenode.net/barman

In case you discover a bug, you can open a ticket using Github:
https://github.com/2ndquadrant-it/barman/issues

### Submitting a bug

Barman has been extensively tested and is currently being used in
several production environments. However, as any software, Barman is
not bug free.

If you discover a bug, please follow this procedure:

- execute the `barman diagnose` command
- file a bug through the Github issue tracker, by attaching the
  output obtained by the diagnostics command above (`barman
  diagnose`)

> **WARNING:**
> Be careful when submitting the output of the diagnose command
> as it might disclose information that are potentially dangerous
> from a security point of view.
