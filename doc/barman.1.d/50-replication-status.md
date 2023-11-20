replication-status *\[OPTIONS\]* *SERVER_NAME*
:   Shows live information and status of any streaming client attached
    to the given server (or servers). Default behaviour can be changed
    through the following options:

    --minimal
    :   machine readable output (default: False)

    --target *TARGET_TYPE*
    :    Possible values for TARGET_TYPE are:

         - *hot-standby*: lists only hot standby servers
         - *wal-streamer*: lists only WAL streaming clients, such as
                          pg_receivewal
         - *all*: any streaming client (default)

    --source *SOURCE_TYPE*
    :    Possible values for SOURCE_TYPE are:

         - *backup-host*: list clients using the backup conninfo for
                          a server (default)
         - *wal-host*: list clients using the WAL streaming conninfo
                       for a server
