keepalive_interval
:   An interval, in seconds, at which a hearbeat query will be sent to the
    server to keep the libpq connection alive during an Rsync backup. Default
    is 60. A value of 0 disables it.

    Scope: Server.