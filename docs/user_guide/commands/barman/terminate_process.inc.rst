.. _commands-barman-terminate-process:

``barman terminate-process``
""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text

    terminate-process SERVER_NAME TASK_NAME

Description
^^^^^^^^^^^^^

The ``barman terminate-process`` command terminates an active Barman subprocess on a
specified server. The target process is identified by its task name (for example, ``backup``
or ``receive-wal``). Note that only processes that are running on the server level can be
terminated, so global processes like ``cron`` or ``config-update`` can not be terminated
by this command.

You can also use the output of ``barman list-processes`` to display all active processes
for a given server and determine which tasks can be terminated. More details about this
command can be found in :ref:`barman list-processes <commands-barman-list-processes>`.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    The name of the server where the subprocess is running.

``TASK_NAME``
    The task name that identifies the subprocess to be terminated.