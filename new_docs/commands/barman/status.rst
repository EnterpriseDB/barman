.. _commands-barman-status:

``barman status``
"""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    status SERVER_NAME

Description
^^^^^^^^^^^

Display information about a server's status, including details such as the state,
Postgres version, WAL information, available backups and more.

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
