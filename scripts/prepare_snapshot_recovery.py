#!/usr/bin/env python3

# © Copyright EnterpriseDB UK Limited 2011-2023
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import logging
import re
import sys
from typing import Any, NamedTuple

from barman.cli import get_server, global_config, parse_backup_id
from barman.fs import unix_command_factory
from barman.cloud_providers.google_cloud_storage import GcpCloudSnapshotInterface

from google.api_core.exceptions import NotFound, GoogleAPICallError
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
    - Mount device to expected path

GCP service account:
default barman vm service account is used.
It was tested with following Compute Admin role. It may be a good idea to use a custom role to limit the scope of actions to the minimum.

Postgres user should be allowed to create directories on mounting path and sudo mount with no password.
    """
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
    try:
        result = operation.result(timeout=timeout)
    except GoogleAPICallError as exc:
        print(exc)
        print(exc.response)
        raise exc

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
        self.snapshot_client = compute.SnapshotsClient()

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
        print(
            f"Retrieve vm instance '{instance_name}' (project:{self.project_id}, zone:{zone})"
        )
        return self.instance_client.get(
            project=self.project_id, zone=zone, instance=instance_name
        )

    def get_snapshot(self, project_id: str, name: str) -> compute.Snapshot:
        """
        Uses snapshot link to retrieve snapshot name and project_name
        :param project_id:
        :param name:
        :return: snapshot
        """
        print(f"retrieve snapshot '{name}' (project:{project_id})")
        return self.snapshot_client.get(project=project_id, snapshot=name)

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
        snapshot_resource: compute.Snapshot,
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
            snapshot_resource: GCP snapshot resource.

        Returns:
            An unattached Disk instance. (google.cloud.compute_v1.types.compute.Disk)
        """
        disk = compute.Disk()
        disk.zone = zone
        disk.size_gb = snapshot_resource.disk_size_gb
        disk.source_snapshot = snapshot_resource.self_link
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
        return compute.AttachedDisk(
            boot=False,
            device_name=device_name,
            kind="compute#attachedDisk",
            source=disk.self_link,
            disk_size_gb=disk.size_gb,
        )

    def attach_disk(
        self, vm_instance_name: str, disk: compute.Disk, zone: str, device_name: str
    ):
        """
        Attach disk to VM instance
        :param vm_instance_name: The vm instance name
        :param disk: the disk resource to attach
        :param zone: The name of the zone
        :param device_name: The name that will be used in the linux operating system
        at that path:/dev/disk/by-id/google-{device_name}
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
            f"Attaching disk '{disk.name}' to vm '{vm_instance_name}' as device '{attached_disk.device_name}' ..."
        )
        wait_for_extended_operation(operation, "attach-disk")


def forge_disk_name(device_name: str, vm_instance_name: str):
    """
    deduce disk name from device_name and vm_instance name
    :param device_name:
    :param vm_instance_name:
    :return:
    """
    disk_name = f"{vm_instance_name}-{device_name}"
    regex = r"[a-z]([-a-z0-9]*[a-z0-9])?"
    if len(disk_name) > 63 or not re.fullmatch(regex, disk_name):
        print(
            f"disk_name {disk_name} does not match requirements: "
            f"'the name must be 1-63 characters long and match the regular expression {regex}'"
        )
        exit(1)
    return disk_name


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


MountOptions = NamedTuple(
    "MountOptions", [("options", str), ("point", str), ("device", str)]
)


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    parser = get_parser()
    args = parser.parse_args()
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

    gcp_compute = GcpComputeFacade(args.project_id)
    snapshots = []
    for snapshot_info in backup_info.snapshots_info.snapshots:
        snapshot = gcp_compute.get_snapshot(
            snapshot_info.snapshot_project,
            snapshot_info.snapshot_name,
        )

        device_name = snapshot_info.device_name
        mount = MountOptions(
            snapshot_info.mount_options,
            snapshot_info.mount_point,
            f"{GcpCloudSnapshotInterface.DEVICE_PREFIX}{device_name}",
        )
        snap = {
            "snapshot_resource": snapshot,
            "device_name": snapshot_info.device_name,
            "disk_name": forge_disk_name(snapshot_info.device_name, args.recovery_node),
            "mount": mount,
        }

        snapshots.append(snap)

    print("Preflight checks")
    print("Validate that disk names do not already exist")
    existing_disks = []
    for snapshot_info in snapshots:
        if gcp_compute.disk_exist(snapshot_info["disk_name"], args.zone):
            disk = gcp_compute.get_disk(snapshot_info["disk_name"], args.zone)
            existing_disks.append(disk.self_link)

    if existing_disks:
        print(f"Following disks already exist {existing_disks}")
        exit(1)

    print("Check mount points")
    remote_command = f"ssh -q postgres@{args.recovery_host}"
    cmd = unix_command_factory(remote_command)
    mount_point_used = []
    for snapshot_info in snapshots:
        device_detected = is_mount_point_used(cmd, snapshot_info["mount"].point)
        if device_detected:
            mount_point_used.append(snapshot_info["mount"].point)
            continue

    if mount_point_used:
        print(f"Following paths already used by device '{mount_point_used}'")
        exit(1)

    print("Prepare disks for recovery")
    for snapshot_info in snapshots:
        # Create Disk from snapshot
        # Close to this command: `gcloud compute disks create  snapshot["disk_name"] --zone args.zone --type disk_type_short --source-snapshot snapshot["snapshot_resource"].self_link --size snapshot["snapshot_resource"].disk_size_gb`
        disk = gcp_compute.create_disk_from_snapshot(
            args.zone,
            snapshot_info["disk_name"],
            disk_type,
            snapshot_info["snapshot_resource"],
        )
        print(f"Created {disk.self_link}")
        mount = snapshot_info["mount"]
        # Attach disk to vm
        # Close to this command: `gcloud compute instances attach-disk args.recovery_node   --disk disk.name`
        try:
            gcp_compute.attach_disk(
                args.recovery_node, disk, args.zone, get_device_name(disk.name)
            )
        except GoogleAPICallError:
            exit(1)
        # For this postgres user needs to be able to create the directory path
        # needs `chown postgres:postgres /opt/postgres`
        cmd.create_dir_if_not_exists(mount.point)

        # Needs postgres user to sudo mount without password
        # echo "postgres ALL=(root) NOPASSWD: /usr/bin/mount" >> /etc/sudoers
        mount_disk(cmd, mount.device, mount.point, mount.options)

    print("You can now run barman recovery command")


if __name__ == "__main__":
    main()
