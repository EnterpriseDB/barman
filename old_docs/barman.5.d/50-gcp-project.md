gcp_project
:   The ID of the GCP project which owns the instance and storage volumes
    defined by `snapshot_instance` and `snapshot_disks`.
    Required when the `snapshot` value is specified for `backup_method`
    and `snapshot_provider` is set to `gcp`.

    Scope: Global/Server/Model.
