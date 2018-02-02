pre_archive_retry_script
:   Hook script launched before a WAL file is archived by maintenance,
    after 'pre_archive_script'.
    Being this a _retry_ hook script, Barman will retry the execution of the
    script until this either returns a SUCCESS (0), an ABORT_CONTINUE (62) or
    an ABORT_STOP (63) code. Returning ABORT_STOP will propagate the failure at
    a higher level and interrupt the WAL archiving operation. Global/Server.
