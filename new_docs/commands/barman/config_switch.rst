.. _barman_config_switch:

``barman config-switch``
"""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    config-switch SERVER_NAME MODEL_NAME

Description
^^^^^^^^^^^

Apply a set of configuration overrides from the model to the a server in Barman. The final
configuration will combine the server's existing settings with the overrides specified in
the model. 

.. note::
    Only one model can be active at a time for a given server.
    
Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``MODEL_NAME``
    Name of the model.
