immediate_checkpoint
:   This option allows you to control the way PostgreSQL handles
    checkpoint at the start of the backup.
    If set to `false` (default), the I/O workload for the checkpoint
    will be limited, according to the `checkpoint_completion_target`
    setting on the PostgreSQL server. If set to `true`, an immediate
    checkpoint will be requested, meaning that PostgreSQL will complete
    the checkpoint as soon as possible. Global/Server.
