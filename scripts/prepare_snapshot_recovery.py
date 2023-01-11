#!/usr/bin/env python3


# To be used only with python 3.7 or more recent version
import argparse
import logging
import re
import sys
from typing import Any

from barman.cli import get_server, global_config, parse_backup_id
from barman.fs import unix_command_factory
from barman.cloud_providers.google_cloud_storage import GcpCloudSnapshotInterface

from google.api_core.exceptions import GoogleAPIError, Conflict, NotFound
from google.api_core.extended_operation import ExtendedOperation
from google.cloud import compute

if sys.version_info.major < 3 or sys.version_info.minor < 7:
    print("Minimal python version is 3.7")
    exit(1)


def get_parser():
    description = """Prepare postgres to recover from Barman snapshot.
This script should run from barman server as barman user.
It does following steps:
    - Look for snapshot info in backup
    - Verify recovery node exists
    - Check that mounted path are not used in recovery node
    - Create disks from snapshot
    - Attach disks to recovery node
    - Mount device to expected path"""
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-s",
        "--server-name",
        help="Server name where the backup is (the server name defined in barman config)",
        required=True,
    )
    parser.add_argument(
        "-b", "--backup-id", help="Barman backup id to recover from", required=True
    )
    parser.add_argument(
        "-n",
        "--recovery-node",
        help="GCP node name that will receive snapshot disks",
        required=True,
    )
    parser.add_argument(
        "-r",
        "--recovery-host",
        help="GCP node hostname so execute remote ssh command from barman user",
        required=True,
    )
    parser.add_argument("-p", "--project-id", help="Google project id", required=True)
    parser.add_argument(
        "-z", "--zone", help="GCP zone used for vm and disk", required=True
    )
    return parser


def get_backup_info(args):
    """
    Load barman config and retrieve backup info

    :param args: needs backup_id in args
    :return:
    """
    ns = argparse.Namespace(
        format="console",
        quiet=True,
        debug=True,
        color="auto",
    )
    global_config(ns)
    server_args = argparse.Namespace(server_name=args.server_name)
    print(server_args)
    server = get_server(server_args)
    return parse_backup_id(server, args)


# from https://cloud.google.com/compute/docs/disks/restore-snapshot#python
def wait_for_extended_operation(
    operation: ExtendedOperation, verbose_name: str = "operation", timeout: int = 300
) -> Any:
    """
    This method will wait for the extended (long-running) operation to
    complete. If the operation is successful, it will return its result.
    If the operation ends with an error, an exception will be raised.
    If there were any warnings during the execution of the operation
    they will be printed to sys.stderr.

    Args:
        operation: a long-running operation you want to wait on.
        verbose_name: (optional) a more verbose name of the operation,
            used only during error and warning reporting.
        timeout: how long (in seconds) to wait for operation to finish.
            If None, wait indefinitely.

    Returns:
        Whatever the operation.result() returns.

    Raises:
        This method will raise the exception received from `operation.exception()`
        or RuntimeError if there is no exception set, but there is an `error_code`
        set for the `operation`.

        In case of an operation taking longer than `timeout` seconds to complete,
        a `concurrent.futures.TimeoutError` will be raised.
    """
    result = operation.result(timeout=timeout)

    if operation.error_code:
        print(
            f"Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}",
            file=sys.stderr,
            flush=True,
        )
        print(f"Operation ID: {operation.name}", file=sys.stderr, flush=True)
        raise operation.exception() or RuntimeError(operation.error_message)

    if operation.warnings:
        print(f"Warnings during {verbose_name}:\n", file=sys.stderr, flush=True)
        for warning in operation.warnings:
            print(f" - {warning.code}: {warning.message}", file=sys.stderr, flush=True)

    return result


class GcpComputeFacade:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.disk_client = compute.DisksClient()
        self.instance_client = compute.InstancesClient()

    def get_disk(self, disk_name: str, zone: str) -> compute.Disk:
        """

        :param disk_name:
        :param zone:
        :return: compute.Disk
        """
        return self.disk_client.get(project=self.project_id, zone=zone, disk=disk_name)

    def get_vm(self, instance_name: str, zone: str) -> compute.Instance:
        """

        :param instance_name:
        :param zone:
        :return:
        """
        return self.instance_client.get(
            project=self.project_id, zone=zone, instance=instance_name
        )

    def disk_exist(self, disk_name: str, zone: str):
        try:
            self.get_disk(disk_name, zone)
            return True
        except NotFound:
            return False

    def create_disk_from_snapshot(
        self,
        zone: str,
        disk_name: str,
        disk_type: str,
        disk_size_gb: int,
        snapshot_link: str,
    ) -> compute.Disk:
        """
        Default uses vm svc account for creation
        Required 'compute.disks.create' permission

        Creates a new disk in a project in given zone.

        Args:
            # project_id: project ID or project number of the Cloud project you want to use.
            zone: name of the zone in which you want to create the disk.
            disk_name: name of the disk you want to create.
            disk_type: the type of disk you want to create. This value uses the following format:
                "zones/{zone}/diskTypes/(pd-standard|pd-ssd|pd-balanced|pd-extreme)".
                For example: "zones/us-west3-b/diskTypes/pd-ssd"
            disk_size_gb: size of the new disk in gigabytes
            snapshot_link: a link to the snapshot you want to use as a source for the new disk.
                This value uses the following format: "projects/{project_name}/global/snapshots/{snapshot_name}"

        Returns:
            An unattached Disk instance. (google.cloud.compute_v1.types.compute.Disk)
        """
        disk = compute.Disk()
        disk.zone = zone
        disk.size_gb = disk_size_gb
        disk.source_snapshot = snapshot_link
        disk.type_ = disk_type
        disk.name = disk_name
        print(f"Creating disk {disk_name} ...")
        operation = self.disk_client.insert(
            project=self.project_id, zone=zone, disk_resource=disk
        )

        wait_for_extended_operation(operation, "disk creation")

        return self.disk_client.get(project=self.project_id, zone=zone, disk=disk_name)

    def create_attached_disk(
        self, disk: compute.Disk, device_name: str
    ) -> compute.AttachedDisk:
        attached_disk = compute.AttachedDisk(
            boot=False,
            device_name=device_name,
            kind="compute#attachedDisk",
            source=disk.self_link,
            disk_size_gb=disk.size_gb,
        )
        return attached_disk

    def attach_disk(
        self, vm_instance_name: str, disk: compute.Disk, zone: str, device_name: str
    ):
        """

        :param vm_instance_name: The vm instance name
        :param disk: the disk resource to attach
        :param zone: The name of the zone
        :param device_name: The name that will be used in the linux operating system at that path:/dev/disk/by-id/google-{device_name}
        :return:
        """
        attached_disk = self.create_attached_disk(disk, device_name)
        operation = self.instance_client.attach_disk(
            project=self.project_id,
            zone=zone,
            instance=vm_instance_name,
            attached_disk_resource=attached_disk,
        )
        print(
            f"Attaching disk '{disk.name}' to vm '{vm_instance_name}' as device '{attached_disk.device_name}'"
        )
        wait_for_extended_operation(operation, "attach-disk")
        # Todo: check if it succeeds?


def forge_disk_name(device_name: str, vm_instance_name: str):
    """
    deduce disk name from device_name and vm_instance name
    :param device_name:
    :param vm_instance_name:
    :return:
    """
    # Todo: check that we do not exceed max length name and if we do decide the naming
    # The name must be 1-63 characters long, and comply with RFC1035.
    # Specifically, the name must be 1-63 characters long and match the regular expression [a-z]([-a-z0-9]*[a-z0-9])?
    # which means the first character must be a lowercase letter, and all following characters must be
    # a dash, lowercase letter, or digit, except the last character, which cannot be a dash.

    pattern = GcpCloudSnapshotInterface.DEVICE_PREFIX + "(\w+)"
    res = re.findall(pattern, device_name)
    if not res:
        print(
            f"device name '{device_name}' does not match expected pattern '{pattern}'"
        )
    return f"{vm_instance_name}-{res[0]}"


def get_device_name(disk_name):
    return disk_name.split("-")[-1]


def is_mount_point_used(cmd, mount_point):
    """
    Checks if mount_point is mounted
    :param cmd:
    :param mount_point:
    :return: bool
    """
    cmd.cmd("findmnt", args=("-n", "-M", mount_point))
    output = cmd.internal_cmd.out
    if output == "":
        return False
    return True


def mount_disk(cmd, device, mount_point, mount_options):
    print(f"mounting {device} to {mount_point}")
    cmd.cmd("sudo mount", args=("-o", mount_options, device, mount_point))
    out, err = cmd.get_last_output()
    if err:
        print(f"Failed to mount {device} to {mount_point}: {err}")
    return out


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    parser = get_parser()
    args = parser.parse_args()
    # tmp test args
    # args = argparse.Namespace(
    #     server_name="primary",
    #     project_id="barman-324718",
    #     zone="europe-west2-b",
    #     backup_id="20221212T165659",
    #     recovery_host="recovery",
    #     recovery_node="test-snapshots-gcp-recovery",
    # )
    project_id = args.project_id
    disk_type_short = "pd-standard"
    disk_type = f"zones/{args.zone}/diskTypes/{disk_type_short}"

    # Get backup info ( ie snapshot metadata )
    backup_info = get_backup_info(args)
    if not hasattr(backup_info, "snapshots_info"):
        print("No snapshot related to this backup: Nothing to do")
        exit(1)

    snapshot_interface = GcpCloudSnapshotInterface(project_id)
    if not snapshot_interface.instance_exists(args.recovery_node, args.zone):
        print(
            f"Recovery instance {args.recovery_node} in zone {args.zone} is not reachable or does not exists."
        )
        exit(1)

    snapshots = []
    for snapshot_name, snapshot in backup_info.snapshots_info["snapshots"].items():
        snapshot["name"] = snapshot_name
        snapshot["disk_name"] = forge_disk_name(snapshot["device"], args.recovery_node)
        snapshot[
            "snapshot_link"
        ] = f"projects/{project_id}/global/snapshots/{snapshot_name}"
        snapshots.append(snapshot)

    # Preflight checks
    # Validate that disk names do not already exist
    gcp_compute = GcpComputeFacade(args.project_id)
    existing_disks = []
    for snapshot in snapshots:
        if gcp_compute.disk_exist(snapshot["disk_name"], args.zone):
            disk = gcp_compute.get_disk(snapshot["disk_name"], args.zone)
            existing_disks.append(disk.self_link)

    if existing_disks:
        print(f"Following disks already exist {existing_disks}")
        exit(1)

    # Check mount points
    remote_command = f"ssh -q postgres@{args.recovery_host}"
    cmd = unix_command_factory(remote_command)
    mount_point_used = []
    for snapshot in snapshots:
        device_detected = is_mount_point_used(cmd, snapshot["mount_point"])
        if device_detected:
            mount_point_used.append(snapshot["mount_point"])
            continue

    if mount_point_used:
        print(f"Following paths already used by device '{mount_point_used}'")
        exit(1)

    # loop over snapshots metadata and prepare disks
    for snapshot in snapshots:
        # Create Disk from snapshot
        # Close to this command: `gcloud compute disks create  snapshot["disk_name"] --zone args.zone --type disk_type_short --source-snapshot snapshot["snapshot_link"] --size snapshot["size"]`
        disk = gcp_compute.create_disk_from_snapshot(
            args.zone,
            snapshot["disk_name"],
            disk_type,
            snapshot["size"],
            snapshot["snapshot_link"],
        )
        print(f"Created {disk.self_link}")

        # Attach disk to vm
        # Close to this command: `gcloud compute instances attach-disk args.recovery_node   --disk disk.name`
        gcp_compute.attach_disk(
            args.recovery_node, disk, args.zone, get_device_name(disk.name)
        )
        # For this postgres user needs to be able to create the directory path
        # needs `chown postgres:postgres /opt/postgres`
        cmd.create_dir_if_not_exists(snapshot["mount_point"])

        # Needs postgres user to sudo mount without password
        # echo "postgres ALL=(root) NOPASSWD: /usr/bin/mount" >> /etc/sudoers
        mount_disk(
            cmd, snapshot["device"], snapshot["mount_point"], snapshot["mount_options"]
        )

    print("You can now run barman recovery command ")


if __name__ == "__main__":
    main()
