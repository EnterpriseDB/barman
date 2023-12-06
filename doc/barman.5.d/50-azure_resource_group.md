azure_resource_group
:   The name of the Azure resource group to which the compute instance and
    disks defined by `snapshot_instance` and `snapshot_disks` belong.
    Required when the `snapshot` value is specified for `backup_method` and
    `snapshot_provider` is set to `azure`.

    Scope: Global/Server/Model.
