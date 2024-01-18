config-update *JSON_CHANGES*
:   Create or update configuration of servers and/or models in Barman.
    `JSON_CHANGES` should be a JSON string containing an array of documents.
    Each document must contain the `scope` key, which can be either
    `server` or `model`, and either the `server_name` or `model_name` key,
    depending on the value of `scope`. Besides that, other keys are
    expected to be Barman configuration options along with their desired
    values.
