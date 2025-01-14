.. _commands-barman-cron:

``barman cron``
"""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    cron
        [ { -h | --help } ]
        [ --keep-descriptors ]

Description
^^^^^^^^^^^

Carry out maintenance tasks, such as enforcing retention policies or managing WAL files.

Parameters
^^^^^^^^^^

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``--keep-descriptors``
    Keep the ^stdout^ and ^stderr^ streams of the Barman subprocesses connected to the
    main process. This is especially useful for Docker-based installations.
