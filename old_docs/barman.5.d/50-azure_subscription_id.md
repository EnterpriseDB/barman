azure_subscription_id
:   The ID of the Azure subscription which owns the instance and storage
    volumes defined by `snapshot_instance` and `snapshot_disks`. Required when
    the `snapshot` value is specified for `backup_method` and
    `snapshot_provider` is set to `azure`.

    Scope: Global/Server/Model.
