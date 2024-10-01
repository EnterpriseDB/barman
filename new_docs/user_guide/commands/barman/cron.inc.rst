.. _commands-barman-cron:

``barman cron``
"""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    cron [ --keep-descriptors ]

Description
^^^^^^^^^^^

Carry out maintenance tasks, such as enforcing retention policies or managing WAL files.

Parameters
^^^^^^^^^^

``--keep-descriptors``
    Keep the ^stdout^ and ^stderr^ streams of the Barman subprocesses connected to the
    main process. This is especially useful for Docker-based installations.
