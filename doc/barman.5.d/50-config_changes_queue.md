config_changes_queue
:   Barman uses a queue to apply configuration changes requested through
    `barman config-update` command. This allows it to serialize multiple
    requests of configuration changes, and also retry an operation which
    has been abruptly terminated. This configuration option allows you
    to specify where in the filesystem the queue should be written. By
    default Barman writes to a file named `cfg_changes.queue` under
    `barman_home`.

    Scope: global.
