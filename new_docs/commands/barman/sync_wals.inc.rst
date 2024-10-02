.. _commands-barman-sync-wals:

``barman sync-wals``
""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    sync-wals SERVER_NAME
    
Description
^^^^^^^^^^^

This command synchronizes a passive node with its primary by copying all archived WAL
files from the server node. It is available only for passive nodes and utilizes the 
``primary_ssh_command`` option to establish a secure connection with the primary node.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node
