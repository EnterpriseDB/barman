.. _commands-barman-config-switch:

``barman config-switch``
""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    config-switch SERVER_NAME { --reset | MODEL_NAME }

Description
^^^^^^^^^^^

Apply a set of configuration overrides from the model to a server in Barman. The final
configuration will combine or override the server's existing settings with the ones
specified in the model. You can reset the server configurations with the ``--reset``
argument.

.. note::
    Only one model can be active at a time for a given server.
    
Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``MODEL_NAME``
    Name of the model.

``--reset``
    Reset the server's configurations.
