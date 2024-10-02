.. _commands-barman-config-update:

``barman config-update``
""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    config-update STRING

Description
^^^^^^^^^^^

Create or update the configurations for servers and/or models in Barman. The parameter
should be a JSON string containing an array of documents. Each document must include a
``scope`` key, which can be either server or model, and either a ``server_name`` or
``model_name`` key, depending on the scope value. Additionally, the document should
include other keys representing Barman configuration options and their desired values.

.. note::
    The barman ``config-update`` command writes configuration options to a file named
    ``.barman.auto.conf``, located in the ``barman_home`` directory. This configuration
    file has higher precedence and will override values from the global Barman
    configuration file (usually ``/etc/barman.conf``) and from any included files specified
    in ``configuration_files_directory`` (typically files in ``/etc/barman.d``). Be aware
    of this if you decide to manually modify configuration options in those files later.

Parameters
^^^^^^^^^^

``STRING``
    List of JSON formatted string.

Example
^^^^^^^

``JSON_STRING='[{“scope”: “server”, “server_name”: “my_server”, “archiver”:
“on”, “streaming_archiver”: “off”}]'``
