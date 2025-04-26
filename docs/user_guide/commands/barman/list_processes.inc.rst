.. _commands-barman-list-processes:

``barman list-processes``
""""""""""""""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text

    list-processes
        [ { -h | --help } ]
        SERVER_NAME

Description
^^^^^^^^^^^

The ``list-processes`` sub-command outputs all active subprocesses for a Barman server.
It displays the process identifier (PID) and the corresponding barman task for each active
subprocess.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server for which to list active subprocesses.

``-h`` / ``--help``
    Displays a help message and exits.