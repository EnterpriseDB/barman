reuse_backup
:   This option controls incremental backup support. Global/Server.
    Possible values are:

    * `off`: disabled (default);
    * `copy`: reuse the last available backup for a server and
    create a copy of the unchanged files (reduce backup time);
    * `link`: reuse the last available backup for a server and
      create a hard link of the unchanged files (reduce backup time
      and space). Requires operating system and file system support
      for hard links.
