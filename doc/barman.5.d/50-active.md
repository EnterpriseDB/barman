active
:   When set to `true` (default), the server is in full operational state.
    When set to `false`, the server can be used for diagnostics, but any
    operational command such as backup execution or WAL archiving is
    temporarily disabled. Setting `active=false` is a good practice
    when adding a new node to Barman. Server.
