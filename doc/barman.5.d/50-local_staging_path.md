local_staging_path
:   A path to a location on the local host where incremental backups will
    be combined during the recovery. This location must have enough
    available space to temporarily hold the new synthetic backup. This 
    option is *required* when recovering from an incremental backup and
    has no effect otherwise.

    Scope: Global/Server/Model.
