.. _barman_show_servers:

``barman show-servers``
"""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    show-servers SERVER_NAME

Description
^^^^^^^^^^^

Display detailed information about a server, including ``conninfo``, ``backup_directory``,
``wals_directory``, ``archive_command``, and many more. To view information about all configured
servers, specify the ``all`` shortcut instead of the server name.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

.. only:: man

    Shortcuts
    ^^^^^^^^^

    .. list-table::
        :widths: 25 100
        :header-rows: 1
    
        * - **Shortcut**
          - **Description**
        * - **all**
          - All available servers
