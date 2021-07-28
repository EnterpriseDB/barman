active
:   When set to `true` (default), the server is in full operational state.
    When set to `false`, the server can be used for diagnostics, but any
    operational command such as backup execution or WAL archiving is
    temporarily disabled. When adding a new server to Barman, we suggest
    setting active=false at first, making sure that barman check shows
    no problems, and only then activating the server. This will avoid
    spamming the Barman logs with errors during the initial setup.
