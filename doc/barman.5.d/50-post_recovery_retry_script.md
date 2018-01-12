post_recovery_retry_script
:   Hook script launched after a recovery.
    Being this a _retry_ hook script, Barman will retry the execution of the
    script until this either returns a SUCCESS (0), an ABORT_CONTINUE (62) or
    an ABORT_STOP (63) code. In a post recovery scenario, ABORT_STOP
    has currently the same effects as ABORT_CONTINUE. Global/Server.
