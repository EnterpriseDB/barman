.. _barman_list_backups:

``barman list-backups``
"""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    list-backups SERVER_NAME

Description
^^^^^^^^^^^

Display the available backups for a server. This command is useful for retrieving both
the backup ID and the backup type. You can find details about this command in
:ref:`catalog`. (TODO - reference the exact part with details)

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

.. only:: man

    Shortcuts
    ^^^^^^^^^

    Use shortcuts instead of ``SERVER_NAME``.

    .. list-table::
        :widths: 25 100
        :header-rows: 1
    
        * - **Shortcut**
          - **Description**
        * - **all**
          - All available servers
