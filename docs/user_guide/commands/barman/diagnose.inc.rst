.. _commands-barman-diagnose:

``barman diagnose``
"""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    diagnose
        [ { -h | --help } ]
        [ --show-config-source ]

Description
^^^^^^^^^^^

Display diagnostic information about the Barman node, which is the server where Barman
is installed, as well as all configured Postgres servers. This includes details such as
global configuration, SSH version, Python version, rsync version, the current
configuration and status of all servers, and many more.

Parameters
^^^^^^^^^^

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``--show-config-source``
    Include the source file which provides the effective value for each configuration
    option.
