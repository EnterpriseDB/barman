cluster
:   Name of the Barman cluster associated with a Barman server or model. Used
    by Barman to group servers and configuration models that can be applied to
    them. Can be omitted for servers, in which case it defaults to the server
    name. Must be set for configuration models, so Barman knows the set of
    servers which can apply a given model.

    Scope: Server/Model.
