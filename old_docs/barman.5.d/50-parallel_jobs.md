parallel_jobs
:   This option controls how many parallel workers will copy files during a
    backup or recovery command. Default 1. For backup purposes, it works only
    when `backup_method` is `rsync`.

    Scope: Global/Server/Model.
