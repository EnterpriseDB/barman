pre_delete_retry_script
:   Hook script launched before the deletion of a backup, after 'pre_delete_script'.
    Being this a _retry_ hook script, Barman will retry the execution of the
    script until this either returns a SUCCESS (0), an ABORT_CONTINUE (62) or
    an ABORT_STOP (63) code. Returning ABORT_STOP will propagate the failure at
    a higher level and interrupt the backup deletion. Global/Server.
