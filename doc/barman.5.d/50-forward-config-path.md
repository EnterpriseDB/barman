forward_config_path
:   Parameter which determines whether a passive node should forward its
    configuration file path to its primary node during cron or sync-info
    commands. Set to true if you are invoking barman with the `-c/--config`
    option and your configuration is in the same place on both the passive
    and primary barman servers. Defaults to false.
