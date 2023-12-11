config-switch *SERVER_NAME* *MODEL_NAME*
:   Apply a set of configuration overrides defined in the model ``MODEL_NAME``
    to the Barman server ``SERVER_NAME``. The final configuration is composed
    of the server configuration plus the overrides defined in the given model.
    Note: there can only be at most one model active at a time for a given
    server.