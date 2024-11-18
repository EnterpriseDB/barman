# CONFIGURATION FILE DIRECTORY

Barman supports the inclusion of multiple configuration files, through
the `configuration_files_directory` option. Included files must contain
only server specifications, not global configurations.
If the value of `configuration_files_directory` is a directory, Barman reads
all files with `.conf` extension that exist in that folder.
For example, if you set it to `/etc/barman.d`, you can
specify your PostgreSQL servers placing each section in a separate `.conf`
file inside the `/etc/barman.d` folder.
