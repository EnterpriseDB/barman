archiver_batch_size
:   This option allows you to activate batch processing of WAL files
    for the `archiver` process, by setting it to a value > 0. Otherwise,
    the traditional unlimited processing of the WAL queue is enabled.
    When batch processing is activated, the `archive-wal` process would
    limit itself to maximum `archiver_batch_size` WAL segments per single
    run. Integer. Global/Server.
