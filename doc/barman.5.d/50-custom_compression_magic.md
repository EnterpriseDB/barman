custom_compression_magic
:   Customised compression magic which is checked in the beginning of a
    WAL file to select the custom algorithm. If you are using a
    custom compression filter then setting this will prevent barman from
    applying the custom compression to WALs which have been
    pre-compressed with that compression. If you do not configure this
    then custom compression will still be applied but any pre-compressed
    WAL files will be compressed again during WAL archive. Global/Server.
