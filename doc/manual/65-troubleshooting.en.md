\newpage

# Troubleshooting

## Diagnose a Barman installation

You can gather important information about all the configured server
using:

``` bash
barman diagnose
```

The `diagnose` command also provides other useful information, such as
global configuration, SSH version, Python version, `rsync` version,
PostgreSQL clients version, as well as current configuration and
status of all servers.

## Requesting for help

TODO: Mention the mailing list

## Submitting a bug

Barman has been extensively tested, and is currently being used in
several production environments. However, as any software, Barman is
not bug free.

If you discover a bug, please follow this procedure:

- execute the `barman diagnose` command;
- file a bug through the Github issue tracker, by attaching the
  output obtained by the diagnostics command above (`barman
  diagnose`).

> **WARNING:**
> Be careful when submitting the output of the diagnose command
> as it might disclose information that are potentially dangerous
> from a security point of view.
