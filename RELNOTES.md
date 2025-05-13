# Barman release notes

© Copyright EnterpriseDB UK Limited 2025 - All rights reserved.

## 3.14.0 (2025-05-15)

### Notable changes

- Implementation of GPG encryption for tar backups and WAL files

  Implement GPG encryption of tar backups. Encryption starts at the end of the backup,
  encrypting the backup of PGDATA and tablespaces present in the backup directory.
  Encrypted backup files will have the `.gpg` extension added.

  Barman supports the decryption and restoration of GPG-encrypted backups using a
  passphrase obtained through the new `encryption_passphrase_command` configuration
  option. During the restore process, decrypted files are staged in the `local_staging_path`
  setting on the Barman host, ensuring a reliable and safe restore process.

  New configuration options required for encryption and decryption of backups
  and WAL files needed to be added. The new options are `encryption`,
  `encryption_key_id`, and `encryption_passphrase_command`.

  WAL files are all encrypted with GPG when `encryption = gpg`. This includes
  changing the way that xlogdb records are read and written (maintaining backwards
  compatibility), and a new logic to detect when files are encrypted and the
  encryption process itself.

  Decryption of GPG-encrypted WAL files during the restore process when using the
  get-wal and no-get-wal flags of the barman restore command. This extends the
  functionality added for decrypting backups via the
  `encryption_passphrase_command` configuration option.

  There's a new field in `show-backup` to expose if a backup was encrypted, and
  specifies the encryption method that was used, if any.

  The `barman check` command verifies if the user's encryption settings are
  correctly configured in the Barman server and functioning as expected.

  References: BAR-683, BAR-687, BAR-693, BAR-669, BAR-671, BAR-692, BAR-685, BAR-680, BAR-670, BAR-681, BAR-702.

### Minor changes

- Allow compression level to be specified for WAL compression in Barman server

  Add a new `compression_level` parameter to the Barman configuration.
  This option accepts a valid integer value or one of the predefined options:
  `low`, `medium`, and `high`. Each option corresponds to a different
  level depending on the compression algorithm chosen.

  References: BAR-540.

- Add client-side compression to `barman-wal-archive`

  Client-side compression options have been added to `barman-wal-archive`,
  supporting the same algorithms that are available on a Barman server.
  When enabled, compression is applied on the client side before sending the WAL to
  the Barman server. The `--compression-level` parameter allows specifying a desired
  compression level for the chosen algorithm.

  References: BAR-262.

- Add --compression-level parameter to barman-cloud-wal-archive

  A parameter called `compression-level` was added to `barman-cloud-wal-archive`,
  which allows a level to be specified for the compression algorithm in use.

  References: BAR-557.

- Add Snappy compression algorithm to Barman server

  The Snappy compression, previously only available in `barman-cloud-wal-archive`, is
  now also available for standard Barman server. As with all other algorithms, it can
  be configured by setting `snappy` in the `compression` configuration parameter.

  References: BAR-557.

- Introduce the new `list-processes` sub-command for listing the server processes

  Add a new `list-processes` command that outputs all active subprocesses for
  a Barman server. The command displays each process's PID and task.

  References: BAR-664.

- Introduce the new `terminate-process` sub-command for terminating Barman subprocesses

  Add a new `terminate-process` command that allows users to terminate an active
  Barman subprocess for a given server by specifying its task name. Barman will
  terminate the subprocess as long as it belongs to the specified server and it is
  currently active.

  References: BAR-665.

- Remove the pin from boto3 version used in cloud scripts

  After thorough investigation of issues with boto3 >= 1.36, we've
  decided to remove the pin that kept the dependency at version
  1.35.

  Both AWS and MinIO object stores work correctly with the latest version, and using
  a version of boto3 that is >= 1.36 ensures the Barman cloud scripts work in a
  FIPS-compliant environment.

  References: BAR-637.

### Bugfixes

- Ensure minimum redundancy check considers only 'non-incremental backups'

  An issue was reported where the `minimum_redundancy` rule could be violated due to
  the inclusion of incremental backups in the redundancy count. As an example: in a
  scenario where the catalog contained one full backup and two dependent incremental
  backups, and the user had `minimum_redundancy = 2`, the rule was incorrectly
  considered satisfied. As a result, deleting the full backup triggered cascading
  deletion of its incremental dependents, leaving zero backups in the catalog.

  This issue has been fixed by updating the `minimum_redundancy` logic to consider
  only non-incremental backups (i.e. only full, rsync, snapshot). This ensures that
  full backups cannot be deleted if doing so would violate the configured minimum
  redundancy level.

  References: BAR-707.

- Fix usage of `barman-wal-restore` with `--keep-compression` using `gzip`, `bzip2`, and `pigz` compression algorithms

  Fix an issue in `barman-wal-restore` where, when trying to restore WALs
  compressed with `gzip`, `bzip2` or `pigz` while having `--keep-compression`
  specified, leading to unexpected errors.

  References: BAR-722.

## 3.13.3 (2025-04-24)

### Bugfixes

- Fix local restore of block-level incremental backups

  When performing a local restore of block-level incremental backups, Barman was
  facing errors like the following:

  ```text
  ERROR: Destination directory '/home/vagrant/restore/internal_no_get_wal' must be empty
  ```

  That error was caused by a regression when the option `--staging-wals-directory`
  was introduced in version 3.13.0. Along with it came a new check to ensure the WAL
  destination directory was empty before proceeding. However, when restoring
  block-level incremental backups locally, Barman was setting up the WAL destination
  directory before performing this check, triggering the error above.

  References: BAR-655.

- Fix regression when running `barman-cloud-backup` as a hook

  Barman 3.13.2 changed the location of the `backup.info` metadata file as part
  of the work delivered to fix issues in WORM environments.

  However, those changes introduced a regression when using `barman-cloud-backup`
  as a backup hook in the Barman server: the hook was not aware of the new location
  of the metadata file.

  This update fixes that issue, so `barman-cloud-backup` becomes aware of the new
  folder structure, and properly locates the `backup.info` file, avoiding runtime
  failures.

  References: BAR-696.

- Avoid decompressing partial WAL files when custom compression is configured

  Fixed an issue where Barman attempted to decompress partial WAL files when
  custom compression was configured. Partial WAL files are never compressed,
  so any attempt to decompress them is incorrect and caused errors when using
  the `--partial` flag with `barman-wal-restore` or `barman get-wal`.

  References: BAR-697.

- Fixed `barman-cloud-backup` not recycling temporary part files

  This fixes a `barman-cloud-backup` problem where temporary part files were not
  deleted after being uploaded to the cloud, leading to disk space exhaustion.
  The issue happened only when using Python >= 3.12 and it was due to a change
  in Python that removed the `delete` attribute of named-temporary file
  objects, which Barman used to rely on when performing internal checks.

  References: BAR-674.

- Fixed backup annotations usage in WORM environments

  Barman previously stored backup annotation files, used to track operations like
  `barman keep` and `barman delete`, inside the backup directory itself. These
  annotations help determine whether a backup should be kept or marked for deletion.
  However, in WORM environments, files in the backup directory cannot be modified or
  deleted after a certain period, which caused issues with managing backup states.
  This fix relocates annotation files to a dedicated metadata directory, as to
  ensure that such operations function correctly in WORM environments.

  References: BAR-663.

## 3.13.2 (2025-03-27)

### Minor changes

- Fix errors when using an immutable storage

  Added a new `worm_mode` configuration to enable WORM (Write Once Read Many)
  handling in Barman, allowing it to support backups on immutable storage.

  This fix also provides automatic relocation of the backup.info file in a new
  directory `meta` inside `backup_directory`. This will let Barman update it
  in future when needed.

  Barman will also _not_ purge the wals directory for WAL files that are not
  needed when running the first backup. This will add some extra space
  which will be reclaimed when this first backup is obsolete and removed
  (by that time, the backups and the WALs will be outside the retention
  policy window).

  Added additional notes to the documentation explaining limitations when
  running with an immutable storage for backups. In particular the need
  for a grace period in the immutability of files and the fact that
  `barman keep` is not supported in these environments.

  References: BAR-649, BAR-645, BAR-650, BAR-651, BAR-652.

## 3.13.1 (2025-03-20)

### Minor changes

- Improve behavior of the backup shortcuts `last-full` / `latest-full`

  The shortcuts `last-full` / `latest-full` were retrieving not the last full backup of
  the server, but the last full backup of the server which was eligible as the parent
  for an incremental backup.

  While this was the expected behavior, the feedback from the community has shown that
  it was confusing for the users.

  From now on, the shortcuts `last-full` / `latest-full` will retrieve the last full
  backup of the Barman server, independently if that backup is eligible as the parent
  for an incremental backup or not.

  The eligibility of the full backup as the parent of an incremental backup will still
  be validated by Barman in a later step, and a proper message will be displayed in
  case it doesn't suit as a parent.

  References: BAR-555.

### Bugfixes

- Fix error message when parsing invalid `--target-time` in `barman restore`

  When using the `barman restore` command, the error message when parsing invalid
  `--target-time` string was:

  ```text
  EXCEPTION: local variable 'parsed_target' referenced before assignment
  ```

  That exception was replaced with an understandable error message.

  References: BAR-627.

- Fix mutual exclusive arguments in the cloud restore command

  In the `barman-cloud-restore` command, we were checking that `target_tli` and
  `target_lsn` were mutually exclusive arguments, where the correct pair to check
  would be `target_time` and `target_lsn`.

  References: BAR-624.

- Fix Barman not honoring `custom_decompression_filter`

  Fixed an issue where Barman was not honoring the configured
  `custom_decompression_filter` if the compression algorithm specified
  was natively supported by Barman. Custom filters now take priority
  over native handlers when decompressing WAL files.

  References: BAR-584.

- Fix barman restore with --no-get-wal and --standby

  Fixed an issue where Barman was removing the `pg_wal` directory during
  recovery if `--no-get-wal` and `--standby-mode` were specified together.
  The issue happened due to Barman incorrectly filling the recovery parameters
  referencing `pg_wal`, including `recovery_end_command`, which led to this
  issue. Barman will now ignore filling such parameters as they are not required
  for this specific case.

  References: BAR-630.

- Fix argument parsing issue in `barman restore` and `barman-cloud-restore`

  In Barman 3.13.0, a regression was introduced causing errors when using
  `barman restore` and `barman-cloud-restore` commands. Specifically, the
  `backup_id` positional argument, which was made optional in that version,
  conflicted with other arguments, causing unrecognized arguments and errors.

  For example, running `barman-cloud-restore` like this:

  ```text
  barman-cloud-restore source_url server_name backup_id --cloud-provider aws-s3 recovery_dir
  ```

  Would trigger an error like this:

  ```text
  barman-cloud-restore: error: unrecognized arguments: recovery_dir
  ```

  This fix resolves the issue by making `backup_id` a required argument
  again. Additionally, a new "auto" value is now accepted as a `backup_id`,
  allowing Barman to automatically choose the best backup for restoration
  without needing a specific `backup_id`. This update fixes argument handling
  and still allows a smooth and flexible restoration process for the user.

  References: BAR-596.

## 3.13.0 (2025-02-20)

### Notable changes

- Add new xlogdb_directory configuration

  Introduces a new `xlogdb_directory` configuration option. This parameter can be
  set either globally or per-server, and allows you to specify a custom directory
  for the `xlog.db` file. This file stores metadata of archived WAL files and is used
  internally by Barman in various scenarios. If unset, it defaults to the value of
  `wals_directory`. Additionally, the file was also renamed to contain the server name
  as a prefix.

  References: BAR-483.

- Make "backup_id" optional when restoring a backup

  Historically, Barman always required a "backup_id" to restore a backup, and would
  use that backup as the source for the restore.

  This feature removes the need for specifying which backup to use as a source for
  restore, making it optional.

  This change applies to both Barman and the barman-cloud scripts.

  Now the user is able to restore a backup in the following ways:
    1. Provide a "backup_id"
    2. Do not provide a "backup_id". It will retrieve the most recent backup
    3. Do not provide a "backup_id", but provide a recovery target, such as:
      - "target_time" (mutually exclusive with target_lsn)
        Will get the closest backup prior to the "target_time"
      - "target_lsn" (mutually exclusive with "target_time")
        Will get the closest backup prior to the "target_lsn"
      - "target_tli" (can be used combined with "target_time" or "target_lsn")
        Will get the most recent backup that matches the timeline. If combined with
        other recovery targets, it will get the most recent backup prior to the
        target_time or target_lsn that matches the timeline

  The recovery targets `--target-xid`, `--target-name` and `--target-immediate`
  are not supported, and will error out with a message if used.

  This feature will provide flexibility and ease when restoring a postgres cluster.

  References: BAR-541, BAR-473.

### Minor changes

- Add current active model to `barman show-server` and `barman status`

  Previously, after applying a configuration model, the only way to check
  which model is currently active for a server was via the `barman diagnose`
  command. With this update, the `barman status` and `barman show-server`
  commands now also display the current active configuration model for a
  server, if any.

  References: BAR-524, BAR-400.

- Add `--staging-wal-directory` option to `barman restore` command to allow alternative WAL directory on PITR

  A new command line option `--staging-wal-directory` was added to the `restore`/`recover`
  command to allow an alternative destination directory for WAL files when performing
  PITR. Previously, WAL files were copied to a `barman_wal` directory within
  the restore destination directory. This enhancement provides greater flexibility, such as
  storing WALs on separate partitions during recovery.

  References: BAR-224.

- Pin boto3 version to any version <= 1.35.99

  Boto3 version 1.36 has changed the way S3 integrity is checked making this version
  incompatible with the current Barman code, generating the following error:

    An error occurred (MissingContentLength) when calling the PutObject operation

  As a temporary workaround, the version for boto3 is pinned to any version <= 1.35.99
  until support for 1.36 is implemented in Barman.

  References: BAR-535.

- Make barman-wal-archive smarter when dealing with duplicate WAL files

  Under some corner cases, Postgres could attempt to archive the same WAL twice.
  For example: if `barman-wal-archive` copies the WAL file over to the Barman host,
  but the script is interrupted before reporting success to Postgres. New executions
  of `barman-wal-archive` could fail when trying to archive the same file again
  because the WAL was already copied from Postgres to Barman, but not yet processed by
  the asynchronous Barman WAL archiver.

  This minor change deals with this situation by verifying the checksum of the
  existing and the incoming file. If the checksums match the incoming file is
  ignored, otherwise an output info message is sent and the incoming file is moved to
  the errors directory. The code will exit with 0 in both situations, avoiding WALs
  piling up in the Postgres host due to a failing `archive_command`.

  References: BAR-225.

- Document procedure to clear WAL archive failure check

  While redesigning the Barman docs we missed adding a note advising
  users to run a `switch-wal` command if the server is idle and
  `barman check` returns a failure on "WAL archiving".

  This addresses the gap left from the previous documentation.

  References: BAR-521.

- Delete WALs by deleting the entire directory at once, when possible

  Previously, when WAL files needed to be deleted (e.g., due to deletion of a backup),
  Barman would iterate over every WAL file and delete them individually. This could
  cause performance issues, mainly in systems which use ZFS filesystem. With this
  change, the entire directory will be deleted whenever noticed that all files in
  the directory are no longer needed by Barman.

  References: BAR-511.

- Add support for `DefaultAzureCredential` option on Azure authentication

  Users can now explicitly use Azure's `DefaultAzureCredential` for authentication
  by using the `default` option for `azure_credential` in the server configuration
  or the `--azure-credential default` option in the case of `barman-cloud-*`.
  Previously, that could only be set as a fallback when no credential was provided
  and no environment variables were set.

  References: BAR-539.

- Improve diagnose output for retention policy info

  Improves the output of the barman diagnose command to display a more user-friendly
  string representations. Specifically, "REDUNDANCY 2" is shown instead of
  "redundancy 2 b" for the 'retention_policy' attribute, and "MAIN" is shown instead
  of "simple-wal 2 b" for the 'wal_retention_policy' attribute.

  References: BAR-100.

### Bugfixes

- Fix PITR when using `barman restore` with `--target-tli`

  Barman was not creating the `recovery.signal` nor filling `recovery_target_timeline`
  in `postgresql.auto.conf` in these cases:

  - The only recovery target passed to `barman restore` was `--target-tli`; or
  - `--target-tli` was specified with some other `--target-*` option, but the
    specified target timeline was the same as the timeline of the chosen backup.

  Now, if any `--target-*` option is passed to `barman restore`, that will be
  correctly treated as PITR.

  References: BAR-543.

- Fix bug when AWS 'profile' variable is referenced before assignment

  An issue was introduced by BAR-242 as part of the Barman 3.12.0 release.
  The issue was causing `barman-cloud-backup-delete` (and possibly other
  commands) to fail with errors like this when `--aws-profile` argument or
  `aws_profile` configuration were not set:

  ```bash
  ERROR: Barman cloud backup delete exception: local
  variable 'profile' referenced before assignment`
  ```

  References: BAR-518.

- Fix --zstd flag on barman-cloud-wal-archive

  Fixed a bug with the `--zstd` flag on `barman-cloud-wal-archive` where it was
  essentially being ignored and not really compressing the WAL file before upload.

  References: BAR-567.

## 3.12.1 (2024-12-09)

### Bugfixes

- Add isoformat fields for backup start and end times in json output

  This patch modifies the json output of the infofile object
  adding two new fields: `begin_time_iso` and `end_time_iso`.
  The new fields allow the use of a more standard and timezone aware
  time format, preserving compatibility with previous versions.
  It is worth noting that in the future the iso format for dates will be the
  standard used by barman for storing dates and will be used everywhere
  non human readable output is requested.

  As part of the work, this patch reverts BAR-316, which was introduced on Barman
  3.12.0.

  References: BAR-494.

## 3.12.0 (2024-11-21)

### Minor changes

- Add FIPS support to Barman

  The `md5` hash algorithm is not FIPS compliant, so it is going to be replaced by
  `sha256`. `sha256` is FIPS compliant, vastly used, and is considered secure for most
  practical purposes.
  Up until this release, Barman's WAL archive client used `hashlib.md5` to generate
  checksums for tar files before they were sent to the Barman server. Here, a tar file is
  a file format used for bundling multiple files together with a `MD5SUMS` file that lists
  the checksums and their corresponding paths.
  In this release, the `md5` hashing algorithm is replaced by `sha256` as the default.
  As a result, checksums for the tar files will be calculated using `sha256`, and the
  `MD5SUMS` file will be named `SHA256SUMS`. Barman still has the ability to use the
  nondefault `md5` algorithm and the `MD5SUMS` file from the client if there is a use
  case for it. The user just needs to add the `--md5` flag to the `barman-wal-archive`
  `archive_command`.

  References: BAR-155, CP-34954, CP-34391.

- Removed el7, debian10, and ubuntu1804 support; updated Debian and SLES.

  Support for el7, debian10, and ubuntu1804 has been removed. Additionally, version 12
  and version name "bookworm" has been added for Debian, addressing a previously
  missing entry. The SLES image version has also been updated from sp4 to sp5.

  References: BAR-389.

- Add support for Postgres Extended 17 (PGE) and Postgres Advanced Server 17 (EPAS)

  Tests were conducted on Postgres Extended 17 (PGE) and Postgres Advanced Server 17
  (EPAS), confirming full compatibility with the latest features in Barman. This
  validation ensures that users of the latest version of PGE and EPAS can leverage all the new
  capabilities of Barman with confidence.

  References: BAR-331.

- Improve WAL compression with `zstd`, `lz4` and `xz` algorithms

  Introduced support for xz compression on WAL files. It can be enabled by specifying
  `xz` in the `compression` server parameter. WALs will be compressed when entering
  the Barman's WAL archive. For the cloud, it can be enabled by specifying `--xz`
  when running `barman-cloud-wal-archive`.

  Introduced support for zstandard compression on WAL files. It can be enabled by
  specifying `zstd` in the `compression` server parameter. WALs will be compressed
  when entering the Barman's WAL archive. For the cloud, it can be enabled by
  specifying `--zstd` when running `barman-cloud-wal-archive`.

  Introduced support for lz4 compression on WAL files. It can be enabled by
  specifying `lz4` in the `compression` server parameter. WALs will be compressed
  when entering the Barman's WAL archive. For the cloud, it can be enabled by
  specifying `--lz4` when running `barman-cloud-wal-archive`.

  References: BAR-265, BAR-423, BAR-264.

- Improve WAL upload performance on S3 buckets by avoiding multipart uploads

  Previously, WAL files were being uploaded to S3 buckets using multipart uploads
  provided by the boto3 library via the `upload_fileobj` method. It was noticed that
  multipart upload is slower when used for small files, such as WAL segments,
  compared to when uploading it in a single PUT request.
  This has been improved by avoiding multipart uploads for files smaller than 100MB.
  The average upload time of each WAL file is expected to be reduced by around 15%
  with this change.

  References: BAR-374.

- Modify behavior when enforcing retention policy for `KEEP:STANDALONE` full backups

  When enforcing the retention policy on full backups created with
  `backup_method = postgres`, Barman was previously marking all dependent (child)
  incremental backups as `VALID`, regardless of the KEEP target used. However, this
  approach is incorrect:

  - For backups labeled `KEEP:STANDALONE`, Barman only retains the WAL files needed to
  restore the server to the exact state of that backup. Because these backups are
  self-contained, any dependent child backups are no longer needed once the root
  backup is outside the retention policy.

  - In contrast, backups marked `KEEP:FULL` are intended for point-in-time recovery.
  To support this, Barman retains all WALs, as well as any child backups, to ensure
  the backup's consistency and allow recovery to the latest possible point.

  This distinction ensures that `KEEP:STANDALONE` backups serve as snapshots of a
  specific moment, while `KEEP:FULL` backups retain everything needed for full
  point-in-time recovery.

  References: BAR-366.

- Update documentation and user-facing features for Barman's recovery process.

  Barman docs and the tool itself used to use the terms "recover"/"recovery" both for
  referencing:

  - The Postgres recovery process;
  - The process of restoring a backup and preparing it for recovery.

  Both the code and documentation have been revised to accurately reflect the usage of
  the terms "restore" and "recover"/"recovery".

  Also, the `barman recover` command was renamed to `barman restore`. The old name is
  still kept as an alias for backward compatibility.

  References: BAR-337.

- Add --keep-compression flag to barman-wal-restore and get-wal

  A new `--keep-compression` option has been added to both `barman-wal-restore` and
  `get-wal`. This option controls whether compressed WAL files should be decompressed
  on the Barman server before being fetched. When specified with `get-wal`, default
  decompression is skipped, and the output is the WAL file content in its original
  state. When specified with `barman-wal-restore`, the WAL file is fetched as-is and,
  if compressed, decompressed on the client side.

  References: BAR-435.

- Ransomware protection - Add AWS Snapshot Lock Support

  Barman now supports AWS EBS Snapshot Lock, a new integrated feature to prevent
  accidental or malicious deletions of Amazon EBS snapshots. When a snapshot is
  locked, it can't be deleted by any user but remains fully accessible for use. This
  feature enables you to store snapshots in WORM (Write-Once-Read-Many) format for a
  specified duration, helping to meet regulatory requirements by keeping the data
  secure and tamper-proof until the lock expires.

  Special thanks to Rui Marinho, our community contributor who started this feature.

  References: BAR-242.

- Prevent orphan files from being left from a crash while deleting a backup

  This commit fixes an issue where backups could leave behind files if the system
  crashed during the deletion of a backup.

  Now, when a backup is deleted, it will get a "delete marker" at the start.
  If a crash happens while the backup is being deleted, the marker will help
  recognize incomplete backup removals when the server restarts.

  The Barman cron job has been updated to look for these deleted markers. If it finds
  a backup with a "delete marker", it will complete the process.

  References: BAR-244.

- Add support for using tags with snapshots

  Barman now supports tagging the snapshots when creating backups using the
  barman-cloud-backup script command. A new argument called --tags was added.

  Special thanks to Rui Marinho, our community contributor who started this feature.

  References: BAR-417.

- Use ISO format instead of ctime when producing JSON output of Barman cloud commands

  The ctime format has no information about the time zone associated with the timestamp.
  Besides that, that format is better suited for human consumption. For machine
  consumption the ISO format is better suited.

  References: BAR-316.

### Bugfixes

- Fix barman check which returns wrong results for Replication Slot

  Previously, when using architectures which backup from a standby node and stream WALs
  from the primary, Barman would incorrectly use `conninfo` (pointing to a standby server)
  for replication checks, leading to errors such as:

  `replication slot (WAL streaming): FAILED (replication slot 'barman' doesn't exist.
  Please execute 'barman receive-wal --create-slot pg17')`

  This fixes the following issue
  [#1024](https://github.com/EnterpriseDB/barman/issues/1024) by ensuring
  `wal_conninfo` is used for WAL replication checks if it's set.

  `wal_conninfo` takes precedence over `wal_streaming_conninfo`, when both are set.
  With this change, if only `wal_conninfo` is set, it will be used and will not fall
  back to `conninfo`.

  Also, in the documentation, changes were made so it is explicit that when `conninfo`
  points to a standby server, `wal_conninfo` must be set and used for accurate
  replication status checks.

  References: BAR-409.

- Fix missing options for `barman keep`

  The error message that the Barman CLI emitted when running `barman keep`
  without any options suggested there were shortcut aliases for status and
  release. These aliases, -s and -r, do not exist, so the error message was
  misleading.
  This fixes the issue by including these short options in the Barman CLI,
  aligning it with other tools like `barman-cloud-backup-keep`, where these
  shortcuts already exist.

  References: BAR-356.

- Lighten standby checks related to conninfo and primary_conninfo

  When backing up a standby server, Barman performs some checks to assert
  that `conninfo` is really pointing to a standby (in recovery mode) and
  that `primary_conninfo` is pointing to a primary (not in recovery).

  The problem, as reported in the issues #704 and #744, is that when a
  failover occurs, the `conninfo` will now be pointing to a primary
  instead and the checks will start failing, requiring the user to change
  Barman configs manually whenever a failover occurs.

  This fix solved the issue by making such checks non-critical, which
  means they will still fail but Barman will keep operating regardless.
  Essentially, Barman will ignore `primary_conninfo` if `conninfo` does
  not point to a standby. Warnings about this misconfiguration will also
  be emitted whenever running any Barman command so the user can be aware.

  References: BAR-348.

- Check for USAGE instead of MEMBER when calling pg_has_role in Barman

  To work correctly Barman database user needs to be included in some roles. Barman was
  verifying the conditions was satisfied by calling `pg_has_role` in Postgres. However,
  it was check for the `MEMBER` privilege instead of `USAGE`. This oversight was fixed.

  This change is a contribution from @RealGreenDragon.

  References: BAR-489.

## 3.11.1 (2024-08-22)

### Bugfixes

- Fix failures in `barman-cloud-backup-delete`. This command was failing when
  applying retention policies due to a bug introduced by the previous release.

## 3.11.0 (2024-08-22)

### Notable changes

- Add support for Postgres 17+ incremental backups. This major feature is
  composed of several small changes:

  - Add `--incremental` command-line option to `barman backup` command. This is
    used to specify the parent backup when taking an incremental backup. The
    parent can be either a full backup or another incremental backup.

  - Add `latest-full` shortcut backup ID. Along with `latest`, this can be used
    as a shortcut to select the parent backup for an incremental backup. While
    `latest` takes the latest backup independently if it is full or incremental,
    `latest-full` takes the latest full backup.

  - `barman keep` command can only be applied to full backups when
    `backup_method = postgres`.

  - Retention policies do not take incremental backups into consideration. As
    incremental backups cannot be recovered without having the complete chain of
    backups available up to the full backup, only full backups account for
    retention policies. If a full backup has dependent incremental backups and the
    retention policy is applied, the full backup will propagate its status to the
    associated incremental backups. When the full backup is flagged with any `KEEP`
    target, Barman will set the status of all related incremental backups to `VALID`.

  - When deleting a backup all the incremental backups depending on it, if any,
    are also removed.

  - `barman recover` needs to combine the full backup with the chain of incremental
    backups when recovering. The new CLI option `--local-staging-path`, and the
    corresponding `local_staging_path` configuration option, are used to specify
    the path in the Barman host where the backups will be combined when recovering
    an incremental backup.

- Changes to `barman show-backup` output:

  - Add the “Estimated cluster size” field. It's useful to have an estimation
    of the data directory size of a cluster when restoring a backup. It’s
    particularly useful when recovering compressed backups or incremental
    backups, situations where the size of the backup doesn’t reflect the size of the
    data directory in Postgres. In JSON format, this is stored as
    `cluster_size`.

  - Add the “WAL summarizer” field. This field shows if `summarize_wal` was
    enabled in Postgres at the time the backup was taken. In JSON format, this
    is stored as `server_information.summarize_wal`. This field is omitted for
    Postgres 16 and older.

  - Add “Data checksums” field. This shows if `data_checkums` was enabled in
    Postgres at the time the backup was taken. In JSON format, this is stored as
    `server_information.data_checksums`.

  - Add the “Backup method” field. This shows the backup method used for this
    backup. In JSON format, this is stored as
    `base_backup_information.backup_method`.

  - Rename the field “Disk Usage” as “Backup Size”. The latter provides a more
    comprehensive name which represents the size of the backup in the Barman
    host. The JSON field under `base_backup_information` was also renamed from
    `disk_usage` to `backup_size`.

  - Add the “WAL size” field. This shows the size of the WALs required by the
    backup. In JSON format, this is stored as
    `base_backup_information.wal_size`.

  - Refactor the field “Incremental size”. It is now named “Resources saving”
    and it now shows an estimation of resources saved when taking incremental
    backups with `rsync` or `pg_basebackup`. It compares the backup size with
    the estimated cluster size to estimate the amount of disk and network
    resources that were saved by taking an incremental backup. In JSON format,
    the field was renamed from `incremental_size` to `resource_savings` under
    `base_backup_information`.

  - Add the `system_id` field to the JSON document. This field contains the
    system identifier of Postgres. It was present in console format, but was
    missing in JSON format.

  - Add fields related with Postgres incremental backups:

    - “Backup type”: indicates if the Postgres backup is full or incremental. In
      JSON format, this is stored as `backup_type` under `base_backup_information`.

    - “Root backup”: the ID of the full backup that is the root of a chain of
      one or more incremental backups. In JSON format, this is stored as
      `catalog_information.root_backup_id`.

    - “Parent backup”: the ID of the full or incremental backup from which this
      incremental backup was taken. In JSON format, this is stored as
      `catalog_information.parent_backup_id`.

    - “Children Backup(s)”: the IDs of the incremental backups that were taken
      with this backup as the parent. In JSON format, this is stored as
      `catalog_information.children_backup_ids`.

    - “Backup chain size”: the number of backups in the chain from this
      incremental backup up to the root backup. In JSON format, this is
      stored as `catalog_information.chain_size`.

- Changes to `barman list-backup` output:

  - It now includes the backup type in the JSON output, which can be either
    `rsync` for backups taken with rsync, `full` or `incremental` for backups
    taken with `pg_basebackup`, or `snapshot` for cloud snapshots. When printing
    to the console the backup type is represented by the corresponding labels
    `R`, `F`, `I` or `S`.

  - Remove tablespaces information from the output. That was bloating the
    output. Tablespaces information can still be found in the output of
    `barman show-backup`.

- Always set a timestamp with a time zone when configuring
  `recovery_target_time` through `barman recover`. Previously, if no time zone
  was explicitly set through `--target-time`, Barman would configure
  `recovery_target_time` without a time zone in Postgres. Without a time zone,
  Postgres would assume whatever is configured through `timezone` GUC in
  Postgres. From now on Barman will issue a warning and configure
  `recovery_target_time` with the time zone of the Barman host if no time zone
  is set by the user through `--target-time` option.

- When recovering a backup with the “no get wal” approach and `--target-lsn` is set,
  copy only the WAL files required to reach the configured target. Previously
  Barman would copy all the WAL files from its archive to Postgres.

- When recovering a backup with the “no get wal” approach and `--target-immediate`
  is set, copy only the WAL files required to reach the consistent point.
  Previously Barman would copy all the WAL files from its archive to Postgres.

- `barman-wal-restore` now moves WALs from the spool directory to `pg_wal`
  instead of copying them. This can improve performance if the spool directory
  and the `pg_wal` directory are in the same partition.

- `barman check-backup` now shows the reason why a backup was marked as `FAILED`
  in the output and logs. Previously for a user to know why the backup was
  marked as `FAILED`, they would need to run `barman show-backup` command.

- Add configuration option `aws_await_snapshots_timeout` and the corresponding
  `--aws-await-snapshots-timeout` command-line option on `barman-cloud-backup`.
  This specifies the timeout in seconds to wait for snapshot backups to reach
  the completed state.

- Add a keep-alive mechanism to rsync-based backups. Previously the Postgres
  session created by Barman to run `pg_backup_start()` and `pg_backup_stop()` would
  stay idle for as long as the base backup copy would take. That could lead to a
  firewall or router dropping the connection because it was idle for a long
  time. The keep-alive mechanism sends heartbeat queries to Postgres
  through that connection, thus reducing the likelihood of a connection
  getting dropped. The interval between heartbeats can be controlled through the new
  configuration option `keepalive_interval` and the corresponding CLI
  option `--keepalive-interval` of the `barman backup` command.

### Bugfixes

- When recovering a backup with the “no get wal” approach and `--target-time`
  set, copy all WAL files. Previously Barman would attempt to “guess” the WAL
  files required by Postgres to reach the configured target time. However,
  the mechanism was not robust enough as it was based on the stats of the WAL
  file in the Barman host (more specifically the creation time). For example:
  if there were archiving or streaming lag between Postgres and Barman, that
  could be enough for recovery to fail because Barman would miss to copy all
  the required WAL files due to the weak check based on file stats.

- Pin `python-snappy` to `0.6.1` when running Barman through Python 3.6 or
older. Newer versions of `python-snappy` require `cramjam` version `2.7.0` or
newer, and these are only available for Python 3.7 or newer.

- `barman receive-wal` now exits with code `1` instead of `0` in the following
  cases:

  - Being unable to run with `--reset` flag because `pg_receivewal` is
    running.

  - Being unable to start `pg_receivewal` process because it is already
    running.

- Fix and improve information about Python in `barman diagnose` output:

  - The command now makes sure to use the same Python interpreter under which
    Barman is installed when outputting the Python version through
    `python_ver` JSON key. Previously, if an environment had multiple Python
    installations and/or virtual environments, the output could eventually be
    misleading, as it could be fetched from a different Python interpreter.

  - Added a `python_executable` key to the JSON output. That contains the path
    to the exact Python interpreter being used by Barman.

## 3.10.1 (2024-06-12)

### Bugfixes

- Make `argcomplete` optional to avoid installation issues on some
  platforms.
- Load `barman.auto.conf` only when the file exists.
- Emit a warning when the `cfg_changes.queue` file is malformed.
- Correct in documentation the postgresql version where
  `pg_checkpoint` is available.
- Add `--no-partial` option to `barman-cloud-wal-restore`.

## 3.10.0 (2024-01-24)

### Notable changes

- Limit the average bandwidth used by `barman-cloud-backup` when backing
  up to either AWS S3 or Azure Blob Storage according to the value set by
  a new CLI option `--max-bandwidth`.

- Add the new configuration option `lock_directory_cleanup`
  That enables cron to automatically clean up the barman_lock_directory
  from unused lock files.

- Add support for a new type of configuration called `model`.
  The model acts as a set of overrides for configuration options
  for a given Barman server.

- Add a new barman command `barman config-update` that allows the creation
  and the update of configurations using JSON

### Bugfixes

- Fix a bug that caused `--min-chunk-size` to be ignored when using
  barman-cloud-backup as hook script in Barman.

## 3.9.0 (2023-10-03)

### Notable changes

- Allow `barman switch-wal --force` to be run against PG>=14 if the
  user has the `pg_checkpoint` role (thanks to toydarian for this patch).

- Log the current check at `info` level when a check timeout occurs.

- The minimum size of an upload chunk when using `barman-cloud-backup`
  with either S3 or Azure Blob Storage can now be specified using the
  `--min-chunk-size` option.

- `backup_compression = none` is supported when using `pg_basebackup`.

- For PostgreSQL 15 and later: the allowed `backup_compression_level`
  values for `zstd` and `lz4` have been updated to match those allowed by
  `pg_basebackup`.

- For PostgreSQL versions earlier than 15: `backup_compression_level = 0`
  can now be used with `backup_compression = gzip`.

### Bugfixes

- Fix `barman recover` on platforms where Multiprocessing uses spawn by
  default when starting new processes.

## 3.8.0 (2023-08-31)

### Notable changes

- Clarify package installation. barman is packaged with default python version
  for each operating system.

- The `minimum-redundancy` option is added to `barman-cloud-backup-delete`.
  It allows to set the minimum number of backups that should always be available.

- Add a new `primary_checkpoint_timeout` configuration option. Allows define
  the amount of seconds that Barman will wait at the end of a backup if no
  new WAL files are produced, before forcing a checkpoint on the primary server.

### Bugfixes

- Fix race condition in barman retention policies application. Backup
  deletions will now raise a warning if another deletion is in progress
  for the requested backup.

- Fix `barman-cloud-backup-show` man page installation.

## 3.7.0 (2023-07-25)

### Notable changes

- Support is added for snapshot backups on AWS using EBS volumes.

- The `--profile` option in the `barman-cloud-*` scripts is renamed
  `--aws-profile`. The old name is deprecated and will be removed in
  a future release.

- Backup manifests can now be generated automatically on completion
  of a backup made with `backup_method = rsync`. This is enabled by
  setting the `autogenerate_manifest` configuration variable and can
  be overridden using the `--manifest` and `--no-manifest` CLI options.

### Bugfixes

- The `barman-cloud-*` scripts now correctly use continuation
  tokens to page through objects in AWS S3-compatible object
  stores. This fixes a bug where `barman-cloud-backup-delete`
  would only delete the oldest 1000 eligible WALs after backup
  deletion.

- Minor documentation fixes.

## 3.6.0 (2023-06-15)

### Notable changes

- PostgreSQL version 10 is no longer supported.

- Support is added for snapshot backups on Microsoft Azure using
  Managed Disks.

- The `--snapshot-recovery-zone` option is renamed `--gcp-zone` for
  consistency with other provider-specific options. The old name
  is deprecated and will be removed in a future release.

- The `snapshot_zone` option and `--snapshot-zone` argument are
  renamed `gcp_zone` and `--gcp-zone` respectively. The old names
  are deprecated and will be removed in a future release.

- The `snapshot_gcp_project` option and `--snapshot-gcp-project`
  argument are renamed to `gcp_project` and `--gcp-project`. The
  old names are deprecated and will be removed in a future release.

### Bugfixes

- Barman will no longer attempt to execute the `replication-status`
  command for a passive node.

- The `backup_label` is deleted from cloud storage when a
  snapshot backup is deleted with `barman-cloud-backup-delete`.

- Man pages for the `generate-manifest` and `verify-backup`
  commands are added.

- Minor documentation fixes.

## 3.5.0 (2023-03-29)

### Notable changes

- Python 2.7 is no longer supported. The earliest Python version
  supported is now 3.6.

- The `barman`, `barman-cli` and `barman-cli-cloud` packages for
  EL7 now require python 3.6 instead of python 2.7. For other
  supported platforms, Barman packages already require python
  versions 3.6 or later so packaging is unaffected.

- Support for PostgreSQL 10 will be discontinued in future Barman
  releases; 3.5.x is the last version of Barman with support for
  PostgreSQL 10.

- Backups and WALs uploaded to Google Cloud Storage can now be
  encrypted using a specific KMS key by using the `--kms-key-name`
  option with `barman-cloud-backup` or `barman-cloud-wal-archive`.

- Backups and WALs uploaded to AWS S3 can now be encrypted using a
  specific KMS key by using the `--sse-kms-key-id` option with
  `barman-cloud-backup` or `barman-cloud-wal-archive` along with
  `--encryption=aws:kms`.

- Two new configuration options are provided which make it possible
  to limit the rate at which parallel workers are started during
  backups with `backup_method = rsync` and recoveries.
  `parallel_jobs_start_batch_size` can be set to limit the amount of
  parallel workers which will be started in a single batch, and
  `parallel_jobs_start_batch_period` can be set to define the time
  in seconds over which a single batch of workers will be started.
  These can be overridden using the arguments `--jobs-start-batch-size`
  and `--jobs-start-batch-period` with the `barman backup` and
  `barman recover` commands.

- A new option `--recovery-conf-filename` is added to `barman recover`.
  This can be used to change the file to which Barman should write the
  PostgreSQL recovery options from the default `postgresql.auto.conf`
  to an alternative location.

### Bugfixes

- Fix a bug which prevented `barman-cloud-backup-show` from
  displaying the backup metadata for backups made with
  `barman backup` and uploaded by `barman-cloud-backup` as a
  post-backup hook script.

- Fix a bug where the PostgreSQL connection used to validate backup
  compression settings was left open until termination of the
  Barman command.

- Fix an issue which caused rsync-concurrent backups to fail when
  running for a duration greater than `idle_session_timeout`.

- Fix a bug where the backup name was not saved in the backup
  metadata if the `--wait` flag was used with `barman backup`.

- Thanks to mojtabash78, mhkarimi1383, epolkerman, barthisrael and
  hzetters for their contributions.

## 3.4.0 (2023-01-26)

### Notable changes

- This is the last release of Barman which will support Python 2 and
  new features will henceforth require Python 3.6 or later.

- A new `backup_method` named `snapshot` is added. This will create
  backups by taking snapshots of cloud storage volumes. Currently
  only Google Cloud Platform is supported however support for AWS
  and Azure will follow in future Barman releases. Note that this
  feature requires a minimum Python version of 3.7. Please see the
  Barman manual for more information.

- Support for snapshot backups is also added to `barman-cloud-backup`,
  with minimal support for restoring a snapshot backup added to
  `barman-cloud-restore`.

- A new command `barman-cloud-backup-show` is added which displays
  backup metadata stored in cloud object storage and is analogous to
  `barman show-backup`. This is provided so that snapshot metadata
  can be easily retrieved at restore time however it is also a
  convenient way of inspecting metadata for any backup made with
  `barman-cloud-backup`.

- The instructions for installing Barman from RPMs in the docs are
  updated.

- The formatting of NFS requirements in the docs is fixed.

- Supported PostgreSQL versions are updated in the docs (this is a
  documentation fix only - the minimum supported major version is
  still 10).

## 3.3.0 (2022-12-14)

### Notable changes

- A backup can now be given a name at backup time using the new
  `--name` option supported by the `barman backup` and
  `barman-cloud-backup` commands. The backup name can then be used
  in place of the backup ID when running commands to interact with
  backups. Additionally, the commands to list and show backups have
  been been updated to include the backup name in the plain text and
  JSON output formats.

- Stricter checking of PostgreSQL version to verify that Barman is
  running against a supported version of PostgreSQL.

### Bugfixes

- Fix inconsistencies between the barman cloud command docs and
  the help output for those commands.

- Use a new PostgreSQL connection when switching WALs on the
  primary during the backup of a standby to avoid undefined
  behaviour such as `SSL error` messages and failed connections.

- Reduce log volume by changing the default log level of stdout
  for commands executed in child processes to `DEBUG` (with the
  exception of `pg_basebackup` which is deliberately logged at
  `INFO` level due to it being a long-running process where it is
  frequently useful to see the output during the execution of the
  command).

## 3.2.0 (2022-10-20)

### Notable changes

- `barman-cloud-backup-delete` now accepts a `--batch-size` option
  which determines the maximum number of objects deleted in a single
  request.

- All `barman-cloud-*` commands now accept a `--read-timeout` option
  which, when used with the `aws-s3` cloud provider, determines the
  read timeout used by the boto3 library when making requests to S3.

### Bugfixes

- Fix the failure of `barman recover` in cases where
  `backup_compression` is set in the Barman configuration but the
  PostgreSQL server is unavailable.

## 3.1.0 (2022-09-14)

### Notable changes

- Backups taken with `backup_method = postgres` can now be compressed
  using lz4 and zstd compression by setting `backup_compression = lz4`
  or `backup_compression = zstd` respectively. These options are only
  supported with PostgreSQL 15 (beta) or later.

- A new option `backup_compression_workers` is available which sets
  the number of threads used for parallel compression. This is
  currently only available with `backup_method = postgres` and
  `backup_compression = zstd`.

- A new option `primary_conninfo` can be set to avoid the need for
  backups of standbys to wait for a WAL switch to occur on the primary
  when finalizing the backup. Barman will use the connection string
  in `primary_conninfo` to perform WAL switches on the primary when
  stopping the backup.

- Support for certain Rsync versions patched for CVE-2022-29154 which
  require a trailing newline in the `--files-from` argument.

- Allow `barman receive-wal` maintenance options (`--stop`, `--reset`,
  `--drop-slot` and `--create-slot`) to run against inactive servers.

- Add `--port` option to `barman-wal-archive` and `barman-wal-restore`
  commands so that a custom SSH port can be used without requiring any
  SSH configuration.

- Various documentation improvements.

- Python 3.5 is no longer supported.

### Bugfixes

- Ensure PostgreSQL connections are closed cleanly during the
  execution of `barman cron`.

- `barman generate-manifest` now treats pre-existing
  backup_manifest files as an error condition.

- backup_manifest files are renamed by appending the backup ID
  during recovery operations to prevent future backups including
  an old backup_manifest file.

- Fix epoch timestamps in json output which were not
  timezone-aware.

- The output of `pg_basebackup` is now written to the Barman
  log file while the backup is in progress.

- We thank barthisrael, elhananjair, kraynopp, lucianobotti, and mxey
  for their contributions to this release.

## 3.0.1 (2022-06-27)

### Bugfixes

- Fix package signing issue in PyPI (same sources as 3.0.0)

## 3.0.0 (2022-06-23)

### Breaking changes

- PostgreSQL versions 9.6 and earlier are no longer
  supported. If you are using one of these versions you will need to
  use an earlier version of Barman.

- The default backup mode for Rsync backups is now
  concurrent rather than exclusive. Exclusive backups have been
  deprecated since PostgreSQL 9.6 and have been removed in PostgreSQL
  15. If you are running Barman against PostgreSQL versions earlier
  than 15 and want to use exclusive backups you will now need to set
  `exclusive_backup` in `backup_options`.

- The backup metadata stored in the `backup.info` file
  for each backup has an extra field. This means that earlier versions
  of Barman will not work in the presence of any backups taken with
  3.0.0. Additionally, users of pg-backup-api will need to upgrade it
  to version 0.2.0 so that pg-backup-api can work with the updated
  metadata.

### Notable changes

- Backups taken with `backup_method = postgres` can now be compressed
  by pg_basebackup by setting the `backup_compression` config option.
  Additional options are provided to control the compression level,
  the backup format and whether the pg_basebackup client or the
  PostgreSQL server applies the compression. NOTE: Recovery of these
  backups requires Barman to stage the compressed files on the recovery
  server in a location specified by the `recovery_staging_path` option.

- Add support for PostgreSQL 15. Exclusive backups are not supported
  by PostgreSQL 15 therefore Barman configurations for PostgreSQL 15
  servers are not allowed to specify `exclusive_backup` in
  `backup_options`.

- Use custom_compression_magic, if set, when identifying compressed
  WAL files. This allows Barman to correctly identify uncompressed
  WALs (such as `*.partial` files in the `streaming` directory) and
  return them instead of attempting to decompress them.

### Minor changes

- Various documentation improvements.

### Bugfixes

- Fix an ordering bug which caused Barman to log the message
  "Backup failed issuing start backup command." while handling a
  failure in the stop backup command.

- Fix a bug which prevented recovery using `--target-tli` when
  timelines greater than 9 were present, due to hexadecimal values
  from WAL segment names being parsed as base 10 integers.

- Fix an import error which occurs when using barman cloud with
  certain python2 installations due to issues with the enum34
  dependency.

- Fix a bug where Barman would not read more than three bytes from
  a compressed WAL when attempting to identify the magic bytes. This
  means that any custom compressed WALs using magic longer than three
  bytes are now decompressed correctly.

- Fix a bug which caused the `--immediate-checkpoint` flag to be
  ignored during backups with `backup_method = rsync`.

## 2.19 (2022-03-09)

### Notable changes

- Change `barman diagnose` output date format to ISO8601.

- Add Google Cloud Storage (GCS) support to barman cloud.

- Support `current` and `latest` recovery targets for the `--target-tli`
  option of `barman recover`.

- Add documentation for installation on SLES.

### Bugfixes

- `barman-wal-archive --test` now returns a non-zero exit code when
  an error occurs.

- Fix `barman-cloud-check-wal-archive` behaviour when `-t` option is
  used so that it exits after connectivity test.

- `barman recover` now continues when `--no-get-wal` is used and
    `"get-wal"` is not set in `recovery_options`.

- Fix `barman show-servers --format=json ${server}` output for
  inactive server.

- Check for presence of `barman_home` in configuration file.

- Passive barman servers will no longer store two copies of the
  tablespace data when syncing backups taken with
  `backup_method = postgres`.

- We thank richyen for his contributions to this release.

## 2.18 (2022-01-21)

### Notable changes

- Add snappy compression algorithm support in barman cloud (requires the
  optional python-snappy dependency).

- Allow Azure client concurrency parameters to be set when uploading
  WALs with barman-cloud-wal-archive.

- Add `--tags` option in barman cloud so that backup files and archived
  WALs can be tagged in cloud storage (aws and azure).

- Update the barman cloud exit status codes so that there is a dedicated
  code (2) for connectivity errors.

- Add the commands `barman verify-backup` and `barman generate-manifest`
  to check if a backup is valid.

- Add support for Azure Managed Identity auth in barman cloud which can
  be enabled with the `--credential` option.

### Bugfixes

- Change `barman-cloud-check-wal-archive` behavior when bucket does
  not exist.

- Ensure `list-files` output is always sorted regardless of the
  underlying filesystem.

- Man pages for barman-cloud-backup-keep, barman-cloud-backup-delete
  and barman-cloud-check-wal-archive added to Python packaging.

- We thank richyen and stratakis for their contributions to this
  release.

## 2.17 (2021-12-01)

### Notable changes

- Resolves a performance regression introduced in version 2.14 which
  increased copy times for `barman backup` or `barman recover` commands
  when using the `--jobs` flag.

- Ignore rsync partial transfer errors for `sender` processes so that
  such errors do not cause the backup to fail (thanks to barthisrael).

## 2.16 (2021-11-17)

### Notable changes

- Add the commands `barman-check-wal-archive` and `barman-cloud-check-wal-archive`
  to validate if a proposed archive location is safe to use for a new PostgreSQL
  server.

- Allow Barman to identify WAL that's already compressed using a custom
  compression scheme to avoid compressing it again.

- Add `last_backup_minimum_size` and `last_wal_maximum_age` options to
  `barman check`.

### Bugfixes

- Use argparse for command line parsing instead of the unmaintained
  argh module.

- Make timezones consistent for `begin_time` and `end_time`.

- We thank chtitux, George Hansper, stratakis, Thoro, and vrms for their
  contributions to this release.

## 2.15 (2021-10-12)

### Notable changes

- Add plural forms for the `list-backup`, `list-server` and
  `show-server` commands which are now `list-backups`, `list-servers`
  and `show-servers`. The singular forms are retained for backward
  compatibility.

- Add the `last-failed` backup shortcut which references the newest
  failed backup in the catalog so that you can do:

  - `barman delete <SERVER> last-failed`

### Bugfixes

- Tablespaces will no longer be omitted from backups of EPAS
  versions 9.6 and 10 due to an issue detecting the correct version
  string on older versions of EPAS.

## 2.14 (2021-09-22)

### Notable changes

- Add the `barman-cloud-backup-delete` command which allows backups in
  cloud storage to be deleted by specifying either a backup ID or a
  retention policy.

- Allow backups to be retained beyond any retention policies in force by
  introducing the ability to tag existing backups as archival backups
  using `barman keep` and `barman-cloud-backup-keep`.

- Allow the use of SAS authentication tokens created at the restricted
  blob container level (instead of the wider storage account level) for
  Azure blob storage

- Significantly speed up `barman restore` into an empty directory for
  backups that contain hundreds of thousands of files.

### Bugfixes

- The backup privileges check will no longer fail if the user lacks
  "userepl" permissions and will return better error messages if any
  required permissions are missing (#318 and #319).

## 2.13 (2021-07-26)

### Notable changes

- Add Azure blob storage support to barman-cloud

- Support tablespace remapping in barman-cloud-restore via
  `--tablespace name:location`

- Allow barman-cloud-backup and barman-cloud-wal-archive to run as
  Barman hook scripts, to allow data to be relayed to cloud storage
  from the Barman server

### Bugfixes

- Stop backups failing due to idle_in_transaction_session_timeout
  <https://github.com/EnterpriseDB/barman/issues/333>

- Fix a race condition between backup and archive-wal in updating
  xlog.db entries (#328)

- Handle PGDATA being a symlink in barman-cloud-backup, which led to
  "seeking backwards is not allowed" errors on restore (#351)

- Recreate pg_wal on restore if the original was a symlink (#327)

- Recreate pg_tblspc symlinks for tablespaces on restore (#343)

- Make barman-cloud-backup-list skip backups it cannot read, e.g.,
  because they are in Glacier storage (#332)

- Add `-d database` option to barman-cloud-backup to specify which
  database to connect to initially (#307)

- Fix "Backup failed uploading data" errors from barman-cloud-backup
  on Python 3.8 and above, caused by attempting to pickle the boto3
  client (#361)

- Correctly enable server-side encryption in S3 for buckets that do
  not have encryption enabled by default.

  In Barman 2.12, barman-cloud-backup's `--encryption` option did
  not correctly enable encryption for the contents of the backup if
  the backup was stored in an S3 bucket that did not have encryption
  enabled. If this is the case for you, please consider deleting
  your old backups and taking new backups with Barman 2.13.

  If your S3 buckets already have encryption enabled by default
  (which we recommend), this does not affect you.

## 2.12.1 (2021-06-30)

### Bugfixes

- Allow specifying target-tli with other `target-*` recovery options.
- Fix incorrect NAME in barman-cloud-backup-list manpage.
- Don't raise an error if SIGALRM is ignored.
- Fetch wal_keep_size, not wal_keep_segments, from Postgres 13.

## 2.12 (2020-11-05)

### Notable changes

- Introduce a new backup_method option called local-rsync which
  targets those cases where Barman is installed on the same server
  where PostgreSQL is and directly uses rsync to take base backups,
  bypassing the SSH layer.

### Bugfixes

- Avoid corrupting boto connection in worker processes.
- Avoid connection attempts to PostgreSQL during tests.

## 2.11 (2020-07-09)

### Notable changes

- Introduction of the barman-cli-cloud package that contains all cloud
  related utilities.

- Add barman-cloud-wal-restore to restore a WAL file previously
  archived with barman-cloud-wal-archive from an object store.

- Add barman-cloud-restore to restore a backup previously taken with
  barman-cloud-backup from an object store.

- Add barman-cloud-backup-list to list backups taken with
  barman-cloud-backup in an object store.

- Add support for arbitrary archive size for barman-cloud-backup.

- Add support for --endpoint-url option to cloud utilities.

- Remove strict superuser requirement for PG 10+ (by Kaarel Moppel).

- Add --log-level runtime option for barman to override default log
  level for a specific command.

- Support for PostgreSQL 13

### Bugfixes

- Suppress messages and warning with SSH connections in barman-cli
  (GH-257).
- Fix a race condition when retrieving uploaded parts in
  barman-cloud-backup (GH-259).
- Close the PostgreSQL connection after a backup (GH-258).
- Check for uninitialized replication slots in receive-wal --reset
  (GH-260).
- Ensure that begin_wal is valorised before acting on it (GH-262).
- Fix bug in XLOG/WAL arithmetic with custom segment size (GH-287).
- Fix rsync compatibility error with recent rsync.
- Fix PostgreSQLClient version parsing.
- Fix PostgreSQL exception handling with non ASCII messages.
- Ensure each postgres connection has an empty search_path.
- Avoid connecting to PostgreSQL while reading a backup.info file.

If you are using already `barman-cloud-wal-archive` or `barman-cloud-backup`
installed via RPM/Apt package and you are upgrading your system, you
must install the barman-cli-cloud package. All cloud related tools are
now part of the barman-cli-cloud package, including
`barman-cloud-wal-archive` and `barman-cloud-backup` that were previously
shipped with `barman-cli`. The reason is complex dependency management of
the boto3 library, which is a requirement for the cloud utilities.

## 2.10 (2019-12-05)

### Notable changes

- Pull .partial WAL files with get-wal and barman-wal-restore,
  allowing restore_command in a recovery scenario to fetch a partial
  WAL file's content from the Barman server. This feature simplifies
  and enhances RPO=0 recovery operations.

- Store the PostgreSQL system identifier in the server directory and
  inside the backup information file. Improve check command to verify
  the consistency of the system identifier with active connections
  (standard and replication) and data on disk.

- A new script called barman-cloud-wal-archive has been added to the
  barman-cli package to directly ship WAL files from PostgreSQL (using
  archive_command) to cloud object storage services that are
  compatible with AWS S3. It supports encryption and compression.

- A new script called barman-cloud-backup has been added to the
  barman-cli package to directly ship base backups from a local
  PostgreSQL server to cloud object storage services that are
  compatible with AWS S3. It supports encryption, parallel upload,
  compression.

- Automated creation of replication slots through the server/global
  option create_slot. When set to auto, Barman creates the replication
  slot, in case streaming_archiver is enabled and slot_name is
  defined. The default value is manual for back-compatibility.

- Add '-w/--wait' option to backup command, making Barman wait for all
  required WAL files to be archived before considering the backup
  completed. Add also the --wait-timeout option (default 0, no
  timeout).

- Redact passwords from Barman output, in particular from
  barman diagnose (InfoSec)

- Improve robustness of receive-wal --reset command, by verifying that
  the last partial file is aligned with the current location or, if
  present, with replication slot's.

- Documentation improvements

### Bugfixes

- Wrong string matching operation when excluding tablespaces
  inside PGDATA (GH-245).
- Minor fixes in WAL delete hook scripts (GH-240).
- Fix PostgreSQL connection aliveness check (GH-239).

## 2.9 (2019-08-01)

### Notable changes

- Transparently support PostgreSQL 12, by supporting the new way of
  managing recovery and standby settings through GUC options and
  signal files (recovery.signal and standby.signal)

- Add --bwlimit command line option to set bandwidth limitation for
  backup and recover commands

- Ignore WAL archive failure for check command in case the latest
  backup is WAITING_FOR_WALS

- Add --target-lsn option to set recovery target Log Sequence Number
  for recover command with PostgreSQL 10 or higher

- Add --spool-dir option to barman-wal-restore so that users can
  change the spool directory location from the default, avoiding
  conflicts in case of multiple PostgreSQL instances on the same
  server (thanks to Drazen Kacar).

- Rename barman_xlog directory to barman_wal

- JSON output writer to export command output as JSON objects and
  facilitate integration with external tools and systems (thanks to
  Marcin Onufry Hlybin). Experimental in this release.

### Bugfixes

- `replication-status` doesn’t show streamers with no slot (GH-222)

- When checking that a connection is alive (“SELECT 1” query),
  preserve the status of the PostgreSQL connection (GH-149). This
  fixes those cases of connections that were terminated due to
  idle-in-transaction timeout, causing concurrent backups to fail.

## 2.8 (2019-05-17)

### Notable changes

- Add support for reuse_backup in geo-redundancy for incremental
  backup copy in passive nodes

- Improve performance of rsync based copy by using strptime instead of
  the more generic dateutil.parser (#210)

- Add ‘--test’ option to barman-wal-archive and barman-wal-restore to
  verify the connection with the Barman server

- Complain if backup_options is not explicitly set, as the future
  default value will change from exclusive_backup to concurrent_backup
  when PostgreSQL 9.5 will be declared EOL by the PGDG

- Display additional settings in the show-server and diagnose
  commands: archive_timeout, data_checksums, hot_standby,
  max_wal_senders, max_replication_slots and wal_compression.

- Merge the barman-cli project in Barman

### Minor changes

- Improve messaging of check --nagios for inactive servers.
- Log remote SSH command with recover command.
- Hide logical decoding connections in replication-status command.

This release officially supports Python 3 and deprecates Python 2 (which
might be discontinued in future releases).

PostgreSQL 9.3 and older is deprecated from this release of Barman.
Support for backup from standby is now limited to PostgreSQL 9.4 or
higher and to WAL shipping from the standby (please refer to the
documentation for details).

### Bugfixes

- Fix encoding error in get-wal on Python 3 (Jeff Janes, #221).
- Fix exclude_and_protect_filter (Jeff Janes, #217).
- Remove spurious message when resetting WAL (Jeff Janes, #215).
- Fix sync-wals error if primary has WALs older than the first
  backup.
- Support for double quotes in synchronous_standby_names setting.

## 2.7 (2019-03-12)

### Notable changes

- Fix error handling during the parallel backup. Previously an
  unrecoverable error during the copy could have corrupted the barman
  internal state, requiring a manual kill of barman process with
  SIGTERM and a manual cleanup of the running backup in PostgreSQL.
  (GH#199).

- Fix support of UTF-8 characters in input and output (GH#194 and
  GH#196).

- Ignore history/backup/partial files for first sync of geo-redundancy
  (GH#198).

- Fix network failure with geo-redundancy causing cron to break
  (GH#202).

- Fix backup validation in PostgreSQL older than 9.2.

- Various documentation fixes.

## 2.6 (2019-02-04)

### Notable changes

- Add support for Geographical redundancy, introducing 3 new commands:
  sync-info, sync-backup and sync-wals. Geo-redundancy allows a Barman
  server to use another Barman server as data source instead of a
  PostgreSQL server.

- Add put-wal command that allows Barman to safely receive WAL files
  via PostgreSQL's archive_command using the barman-wal-archive script
  included in barman-cli.

- Add ANSI colour support to check command.

### Bugfixes

- Fix switch-wal on standby with an empty WAL directory.
- Honour archiver locking in wait_for_wal method.
- Fix WAL compression detection algorithm.
- Fix current_action in concurrent stop backup errors.
- Do not treat lock file busy as an error when validating a backup.

## 2.5 (2018-10-23)

### Notable changes

- Add support for PostgreSQL 11

- Add check-backup command to verify that WAL files required for
  consistency of a base backup are present in the archive. Barman now
  adds a new state (WAITING_FOR_WALS) after completing a base backup,
  and sets it to DONE once it has verified that all WAL files from
  start to the end of the backup exist. This command is included in
  the regular cron maintenance job. Barman now notifies users
  attempting to recover a backup that is in WAITING_FOR_WALS state.

- Allow switch-xlog --archive to work on a standby (just for the
  archive part)

### Bugfixes

- Fix decoding errors reading external commands output (issue
  #174).

- Fix documentation regarding WAL streaming and backup from
  standby.

## 2.4 (2018-05-25)

### Notable changes

- Add standard and retry hook scripts for backup deletion (pre/post).
- Add standard and retry hook scripts for recovery (pre/post).
- Add standard and retry hook scripts for WAL deletion (pre/post).
- Add --standby-mode option to barman recover to add standby_mode = on
  in pre-generated recovery.conf.
- Add --target-action option to barman recover, allowing users to add
  shutdown, pause or promote to the pre-generated recovery.conf file.
- Improve usability of point-in-time recovery with consistency checks
  (e.g. recovery time is after end time of backup).
- Minor documentation improvements.
- Drop support for Python 3.3.

### Bugfixes

- Fix remote get_file_content method (GitHub #151), preventing
  incremental recovery from happening.
- Unicode issues with command (GitHub #143 and #150).
- Add --wal-method=none when pg_basebackup >= 10 (GitHub #133).
- Stop process manager module from overwriting lock files content
- Relax the rules for rsync output parsing
- Ignore vanished files in streaming directory
- Case insensitive slot names (GitHub #170)
- Make DataTransferFailure.from_command_error() more resilient
  (GitHub #86)
- Rename command() to barman_command() (GitHub #118)
- Initialise synchronous standby names list if not set (GitHub #111)
- Correct placeholders ordering (GitHub #138)
- Force datestyle to iso for replication connections
- Returns error if delete command does not remove the backup
- Fix exception when calling is_power_of_two(None)
- Downgraded sync standby names messages to debug (GitHub #89)

## 2.3 (2017-09-05)

### Notable changes

- Add support to PostgreSQL 10

- Follow naming changes in PostgreSQL 10:

  - The switch-xlog command has been renamed to switch-wal.
  - In commands output, the xlog word has been changed to WAL and
    location has been changed to LSN when appropriate.

- Add the --network-compression/--no-network-compression options to
  barman recover to enable or disable network compression at run-time
- Add --target-immediate option to recover command, in order to exit
  recovery when a consistent state is reached (end of the backup,
  available from PostgreSQL 9.4)
- Show cluster state (master or standby) with barman status command
- Documentation improvements

### Bugfixes

- Fix high memory usage with parallel_jobs > 1 (#116)
- Better handling of errors using parallel copy (#114)
- Make barman diagnose more robust with system exceptions
- Let archive-wal ignore files with .tmp extension

## 2.2 (2017-07-17)

### Notable changes

- Implement parallel copy for backup/recovery through the
  parallel_jobs global/server option to be overridden by the --jobs or
  -j runtime option for the backup and recover command. Parallel
  backup is available only for the rsync copy method. By default, it
  is set to 1 (for behaviour compatibility with previous versions).

- Support custom WAL size for PostgreSQL 8.4 and newer. At backup
  time, Barman retrieves from PostgreSQL wal_segment_size and
  wal_block_size values and computes the necessary calculations.

- Improve check command to ensure that incoming directory is empty
  when archiver=off, and streaming directory is empty when
  streaming_archiver=off (#80).

- Add external_configuration to backup_options so that users can
  instruct Barman to ignore backup of configuration files when they
  are not inside PGDATA (default for Debian/Ubuntu installations). In
  this case, Barman does not display a warning anymore.

- Add --get-wal and --no-get-wal options to barman recover

- Add max_incoming_wals_queue global/server option for the check
  command so that a non blocking error is returned in case incoming
  WAL directories for both archiver and the streaming_archiver contain
  more files than the specified value.

- Documentation improvements

- File format changes:

  - The format of backup.info file has changed. For this reason a
    backup taken with Barman 2.2 cannot be read by a previous
    version of Barman. But, backups taken by previous versions can
    be read by Barman 2.2.

### Bugfixes

- Allow replication-status to work against a standby
- Close any PostgreSQL connection before starting pg_basebackup
  (#104, #108)
- Safely handle paths containing special characters
- Archive .partial files after promotion of streaming source
- Recursively create directories during recovery (SF#44)
- Improve xlog.db locking (#99)
- Remove tablespace_map file during recover (#95)
- Reconnect to PostgreSQL if connection drops (SF#82)

## 2.1 (2017-01-05)

### Notable changes

- Add --archive and --archive-timeout options to switch-xlog command.

- Preliminary support for PostgreSQL 10 (#73).

- Minor additions:

  - Add last archived WAL info to diagnose output.
  - Add start time and execution time to the output of delete
    command.

### Bugfixes

- Return failure for get-wal command on inactive server
- Make streaming_archiver_names and streaming_backup_name options
  global (#57)
- Fix rsync failures due to files truncated during transfer (#64)
- Correctly handle compressed history files (#66)
- Avoid de-referencing symlinks in pg_tblspc when preparing
  recovery (#55)
- Fix comparison of last archiving failure (#40, #58)
- Avoid failing recovery if postgresql.conf is not writable (#68)
- Fix output of replication-status command (#56)
- Exclude files from backups like pg_basebackup (#65, #72)
- Exclude directories from other Postgres versions while copying
  tablespaces (#74)
- Make retry hook script options global

## 2.0 (2016-09-27)

### Notable changes

- Support for pg_basebackup and base backups over the PostgreSQL
  streaming replication protocol with backup_method=postgres
  (PostgreSQL 9.1 or higher required)

- Support for physical replication slots through the slot_name
  configuration option as well as the --create-slot and --drop-slot
  options for the receive-wal command (PostgreSQL 9.4 or higher
  required). When slot_name is specified and streaming_archiver is
  enabled, receive-wal transparently integrates with pg_receivexlog,
  and check makes sure that slots exist and are actively used

- Support for the new backup API introduced in PostgreSQL 9.6, which
  transparently enables concurrent backups and backups from standby
  servers using the standard rsync method of backup. Concurrent backup
  was only possible for PostgreSQL 9.2 to 9.5 versions through the
  pgespresso extension. The new backup API will make pgespresso
  redundant in the future

- If properly configured, Barman can function as a synchronous standby
  in terms of WAL streaming. By properly setting the
  streaming_archiver_name in the synchronous_standby_names priority
  list on the master, and enabling replication slot support, the
  receive-wal command can now be part of a PostgreSQL synchronous
  replication cluster, bringing RPO=0 (PostgreSQL 9.5.5 or
  higher required)

- Introduce barman-wal-restore, a standard and robust script written
  in Python that can be used as restore_command in recovery.conf files
  of any standby server of a cluster. It supports remote parallel
  fetching of WAL files by efficiently invoking get-wal through SSH.
  Currently available as a separate project called barman-cli. The
  barman-cli package is required for remote recovery when get-wal is
  listed in recovery_options

- Control the maximum execution time of the check command through the
  check_timeout global/server configuration option (30 seconds
  by default)

- Limit the number of WAL segments that are processed by an
  archive-wal run, through the archiver_batch_size and
  streaming_archiver_batch_size global/server options which control
  archiving of WAL segments coming from, respectively, the standard
  archiver and receive-wal

- Removed locking of the XLOG database during check operations

- The show-backup command is now aware of timelines and properly
  displays which timelines can be used as recovery targets for a given
  base backup. Internally, Barman is now capable of parsing .history
  files

- Improved the logic behind the retry mechanism when copy operations
  experience problems. This involves backup (rsync and postgres) as
  well as remote recovery (rsync)

- Code refactoring involving remote command and physical copy
  interfaces

### Bugfixes

- Correctly handle .history files from streaming
- Fix replication-status on PostgreSQL 9.1
- Fix replication-status when sent and write locations are not
  available
- Fix misleading message on pg_receivexlog termination

## 1.6.1 (2016-05-23)

### Minor changes

- Add --peek option to get-wal command to discover existing WAL files
  from the Barman's archive

- Add replication-status command for monitoring the status of any
  streaming replication clients connected to the PostgreSQL server.
  The --target option allows users to limit the request to only hot
  standby servers or WAL streaming clients

- Add the switch-xlog command to request a switch of a WAL file to the
  PostgreSQL server. Through the '--force' it issues a CHECKPOINT
  beforehand

- Add streaming_archiver_name option, which sets a proper
  application_name to pg_receivexlog when streaming_archiver is
  enabled (only for PostgreSQL 9.3 and above)

- Check for _superuser_ privileges with PostgreSQL's standard
  connections (#30)

- Check the WAL archive is never empty

- Check for 'backup_label' on the master when server is down

- Improve barman-wal-restore contrib script

### Bugfixes

- Treat the "failed backups" check as non-fatal
- Rename '-x' option for get-wal as '-z'
- Add archive_mode=always support for PostgreSQL 9.5 (#32)
- Properly close PostgreSQL connections when necessary
- Fix receive-wal for pg_receive_xlog version 9.2

## 1.6.0 (2016-02-29)

### Notable changes

- Support for streaming replication connection through the
  streaming_conninfo server option

- Support for the streaming_archiver option that allows Barman to
  receive WAL files through PostgreSQL's native streaming protocol.
  When set to 'on', it relies on pg_receivexlog to receive WAL data,
  reducing Recovery Point Objective. Currently, WAL streaming is an
  additional feature (standard log archiving is still required)

- Implement the receive-wal command that, when streaming_archiver is
  on, wraps pg_receivexlog for WAL streaming. Add --stop option to
  stop receiving WAL files via streaming protocol. Add --reset option
  to reset the streaming status and restart from the current xlog
  in Postgres.

- Automatic management (startup and stop) of receive-wal command via
  cron command

- Support for the path_prefix configuration option

- Introduction of the archiver option (currently fixed to on) which
  enables continuous WAL archiving for a specific server, through log
  shipping via PostgreSQL's archive_command

- Support for streaming_wals_directory and errors_directory options

- Management of WAL duplicates in archive-wal command and integration
  with check command

- Verify if pg_receivexlog is running in check command when
  streaming_archiver is enabled

- Verify if failed backups are present in check command

- Accept compressed WAL files in incoming directory

- Add support for the pigz compressor (thanks to Stefano Zacchiroli
  <zack@upsilon.cc>)

- Implement pygzip and pybzip2 compressors (based on an initial idea
  of Christoph Moench-Tegeder <christoph@2ndquadrant.de>)

- Creation of an implicit restore point at the end of a backup

- Current size of the PostgreSQL data files in barman status

- Permit archive_mode=always for PostgreSQL 9.5 servers (thanks to
  Christoph Moench-Tegeder <christoph@2ndquadrant.de>)

- Complete refactoring of the code responsible for connecting to
  PostgreSQL

- Improve messaging of cron command regarding sub-processes

- Native support for Python >= 3.3

- Changes of behaviour:
  - Stop trashing WAL files during archive-wal (commit:e3a1d16)

### Bugfixes

- Atomic WAL file archiving (#9 and #12)
- Propagate "-c" option to any Barman subprocess (#19)
- Fix management of backup ID during backup deletion (#22)
- Improve archive-wal robustness and log messages (#24)
- Improve error handling in case of missing parameters

## 1.5.1 (2015-11-16)

### Minor changes

- Add support for the 'archive-wal' command which performs WAL
  maintenance operations on a given server
- Add support for "per-server" concurrency of the 'cron' command
- Improved management of xlog.db errors
- Add support for mixed compression types in WAL files (SF.net#61)

### Bugfixes

- Avoid retention policy checks during the recovery
- Avoid 'wal_level' check on PostgreSQL version < 9.0 (#3)
- Fix backup size calculation (#5)

## 1.5.0 (2015-09-28)

### Notable changes

- Add support for the get-wal command which allows users to fetch any
  WAL file from the archive of a specific server
- Add support for retry hook scripts, a special kind of hook scripts
  that Barman tries to run until they succeed
- Add active configuration option for a server to temporarily disable
  the server by setting it to False
- Add barman_lock_directory global option to change the location of
  lock files (by default: 'barman_home')
- Execute the full suite of checks before starting a backup, and skip
  it in case one or more checks fail
- Forbid to delete a running backup
- Analyse include directives of a PostgreSQL server during backup and
  recover operations
- Add check for conflicting paths in the configuration of Barman, both
  intra (by temporarily disabling a server) and inter-server (by
  refusing any command, to any server).
- Add check for wal_level
- Add barman-wal-restore script to be used as restore_command on a
  standby server, in conjunction with barman get-wal
- Implement a standard and consistent policy for error management
- Improved cache management of backups
- Improved management of configuration in unit tests
- Tutorial and man page sources have been converted to Markdown format
- Add code documentation through Sphinx
- Complete refactor of the code responsible for managing the backup
  and the recover commands
- Changed internal directory structure of a backup
- Introduce copy_method option (currently fixed to rsync)

### Bugfixes

- Manage options without '=' in PostgreSQL configuration files
- Preserve Timeline history files (Fixes: #70)
- Workaround for rsync on SUSE Linux (Closes: #13 and #26)
- Disables dangerous settings in postgresql.auto.conf
  (Closes: #68)

## 1.4.1 (2015-05-05)

### Minor changes

- Improved management of xlogdb file, which is now correctly fsynced
  when updated. Also, the rebuild-xlogdb command now operates on a
  temporary new file, which overwrites the main one when finished.
- Add unit tests for dateutil module compatibility
- Modified Barman version following PEP 440 rules and added support
  of tests in Python 3.4

### Bugfixes

- Fix for WAL archival stop working if first backup is EMPTY
  (Closes: #64)
- Fix exception during error handling in Barman recovery
  (Closes: #65)
- After a backup, limit cron activity to WAL archiving only
  (Closes: #62)
- Improved robustness and error reporting of the backup delete
  command (Closes: #63)
- Fix computation of WAL production ratio as reported in the
  show-backup command

## 1.4.0 (2015-01-26)

### Notable changes

- Incremental base backup implementation through the reuse_backup
  global/server option. Possible values are off (disabled,
  default), copy (preventing unmodified files from being
  transferred) and link (allowing for deduplication through hard
  links).
- Store and show deduplication effects when using reuse_backup=
  link.
- Added transparent support of pg_stat_archiver (PostgreSQL 9.4) in
  check, show-server and status commands.
- Improved administration by invoking WAL maintenance at the end of
  a successful backup.
- Changed the way unused WAL files are trashed, by differentiating
  between concurrent and exclusive backup cases.
- Improved performance of WAL statistics calculation.
- Treat a missing pg_ident.conf as a WARNING rather than an error.
- Refactored output layer by removing remaining yield calls.
- Check that rsync is in the system path.
- Include history files in WAL management.
- Improved robustness through more unit tests.

### Bugfixes

- Fixed bug #55: Ignore fsync EINVAL errors on directories.
- Fixed bug #58: retention policies delete.

## 1.3.3 (2014-08-21)

### Notable changes

- Added "last_backup_max_age", a new global/server option that
  allows administrators to set the max age of the last backup in a
  catalogue, making it easier to detect any issues with periodical
  backup execution
- Improved robustness of "barman backup" by introducing two global/
  server options: "basebackup_retry_times" and
  "basebackup_retry_sleep". These options allow an administrator to
  specify, respectively, the number of attempts for a copy
  operation after a failure, and the number of seconds of wait
  before retrying
- Improved the recovery process via rsync on an existing directory
  (incremental recovery), by splitting the previous rsync call into
  several ones - invoking checksum control only when necessary
- Added support for PostgreSQL 8.3

### Minor changes

- Support for comma separated list values configuration options
- Improved backup durability by calling fsync() on backup and
  WAL files during "barman backup" and "barman cron"
- Improved Nagios output for "barman check --nagios"
- Display compression ratio for WALs in "barman show-backup"
- Correctly handled keyboard interruption (CTRL-C) while
  performing barman backup
- Improved error messages of failures regarding the stop of a
  backup
- Wider coverage of unit tests

### Bugfixes

- Copies "recovery.conf" on the remote server during "barman
  recover" (#45)
- Correctly detect pre/post archive hook scripts (#41)

## 1.3.2 (2014-04-15)

### Bugfixes

- Fixed incompatibility with PostgreSQL 8.4 (Closes #40, bug
  introduced in version 1.3.1)

## 1.3.1 (2014-04-14)

### Minor changes

- Added support for concurrent backup of PostgreSQL 9.2 and 9.3
  servers that use the "pgespresso" extension. This feature is
  controlled by the "backup_options" configuration option (global/
  server) and activated when set to "concurrent_backup". Concurrent
  backup allows DBAs to perform full backup operations from a
  streaming replicated standby.
- Added the "barman diagnose" command which prints important
  information about the Barman system (extremely useful for support
  and problem solving)
- Improved error messages and exception handling interface

### Bugfixes

- Fixed bug in recovery of tablespaces that are created inside the
  PGDATA directory (bug introduced in version 1.3.0)
- Fixed minor bug of unhandled -q option, for quiet mode of
  commands to be used in cron jobs (bug introduced in version
  1.3.0)
- Minor bug fixes and code refactoring

## 1.3.0 (2014-02-03)

### Notable changes

- Refactored BackupInfo class for backup metadata to use the new
  FieldListFile class (infofile module)

- Refactored output layer to use a dedicated module, in order to
  facilitate integration with Nagios (NagiosOutputWriter class)

- Refactored subprocess handling in order to isolate stdin/stderr/
  stdout channels (command_wrappers module)

- Refactored hook scripts management

- Extracted logging configuration and userid enforcement from the
  configuration class.

- Support for hook scripts to be executed before and after a WAL
  file is archived, through the 'pre_archive_script' and
  'post_archive_script' configuration options.

- Implemented immediate checkpoint capability with
  --immediate-checkpoint command option and 'immediate_checkpoint'
  configuration option

- Implemented network compression for remote backup and recovery
  through the 'network_compression' configuration option (#19)

- Implemented the 'rebuild-xlogdb' command (Closes #27 and #28)

- Added deduplication of tablespaces located inside the PGDATA
  directory

- Refactored remote recovery code to work the same way local
  recovery does, by performing remote directory preparation
  (assuming the remote user has the right permissions on the remote
  server)

- 'barman backup' now tries and create server directories before
  attempting to execute a full backup (#14)

### Bugfixes

- Fixed bug #22: improved documentation for tablespaces relocation

- Fixed bug #31: 'barman cron' checks directory permissions for
  lock file

- Fixed bug #32: xlog.db read access during cron activities

## 1.2.3 (2013-09-05)

### Minor changes

- Added support for PostgreSQL 9.3

- Added support for the "--target-name" recovery option, which allows to
  restore to a named point previously specified with pg_create_restore_point
  (only for PostgreSQL 9.1 and above users)

- Introduced Python 3 compatibility

### Bugfixes

- Fixed bug #27 about flock() usage with barman.lockfile (many thanks to
  Damon Snyder <damonsnyder@users.sf.net>)

## 1.2.2 (2013-06-24)

### Bugfixes

- Fix python 2.6 compatibility

## 1.2.1 (2013-06-17)

### Minor changes

- Added the "bandwidth_limit" global/server option which allows
  to limit the I/O bandwidth (in KBPS) for backup and recovery operations

- Added the "tablespace_bandwidth_limit" global/server option which allows
  to limit the I/O bandwidth (in KBPS) for backup and recovery operations
  on a per tablespace basis

- Added /etc/barman/barman.conf as default location

### Bugfixes

- Avoid triggering the minimum_redundancy check
  on FAILED backups (thanks to Jérôme Vanandruel)

## 1.2.0 (2013-01-31)

### Notable changes

- Added the "retention_policy_mode" global/server option which defines
  the method for enforcing retention policies (currently only "auto")

- Added the "minimum_redundancy" global/server option which defines
  the minimum number of backups to be kept for a server

- Added the "retention_policy" global/server option which defines
  retention policies management based on redundancy (e.g. REDUNDANCY 4)
  or recovery window (e.g. RECOVERY WINDOW OF 3 MONTHS)

- Added retention policy support to the logging infrastructure, the
  "check" and the "status" commands

- The "check" command now integrates minimum redundancy control

- Added retention policy states (valid, obsolete and potentially obsolete)
  to "show-backup" and "list-backup" commands

- The 'all' keyword is now forbidden as server name

- Added basic support for Nagios plugin output to the 'check'
  command through the --nagios option

- Barman now requires argh => 0.21.2 and argcomplete-

- Minor bug fixes

## 1.1.2 (2012-11-29)

### Minor changes

- Added "configuration_files_directory" option that allows
  to include multiple server configuration files from a directory

- Support for special backup IDs: latest, last, oldest, first

- Management of  multiple servers to the 'list-backup' command.
  'barman list-backup all' now list backups for all the configured servers.

- Added "application_name" management for PostgreSQL >= 9.0

### Bugfixes

- Fixed bug #18: ignore missing WAL files if not found during delete

## 1.1.1 (2012-10-16)

### Bugfixes

- Fix regressions in recover command.

## 1.1.0 (2012-10-12)

### Notable changes

- Support for hook scripts to be executed before and after
  a 'backup' command through the 'pre_backup_script' and 'post_backup_script'
  configuration options.

- Management of  multiple servers to the 'backup' command.
  'barman backup all' now iteratively backs up all the configured servers.

- Add warning in recovery when file location options have been defined
  in the postgresql.conf file (issue #10)

- Fail fast on recover command if the destination directory contains
  the ':' character (Closes: #4) or if an invalid tablespace
  relocation rule is passed

- Report an informative message when pg_start_backup() invocation
  fails because an exclusive backup is already running (Closes: #8)

### Bugfixes

- Fixed bug #9: "9.2 issue with pg_tablespace_location()"

## 1.0.0 (2012-07-06)

### Notable changes

- Backup of multiple PostgreSQL servers, with different versions. Versions
  from PostgreSQL 8.4+ are supported.

- Support for secure remote backup (through SSH)

- Management of a catalog of backups for every server, allowing users
  to easily create new backups, delete old ones or restore them

- Compression of WAL files that can be configured on a per server
  basis using compression/decompression filters, both predefined (gzip
  and bzip2) or custom

- Support for INI configuration file with global and per-server directives.
  Default location for configuration files are /etc/barman.conf or
  ~/.barman.conf. The '-c' option allows users to specify a different one

- Simple indexing of base backups and WAL segments that does not require
  a local database

- Maintenance mode (invoked through the 'cron' command) which performs
  ordinary operations such as WAL archival and compression, catalog
  updates, etc.

- Added the 'backup' command which takes a full physical base backup
  of the given PostgreSQL server configured in Barman

- Added the 'recover' command which performs local recovery of a given
  backup, allowing DBAs to specify a point in time. The 'recover' command
  supports relocation of both the PGDATA directory and, where applicable,
  the tablespaces

- Added the '--remote-ssh-command' option to the 'recover' command for
  remote recovery of a backup. Remote recovery does not currently support
  relocation of tablespaces

- Added the 'list-server' command that lists all the active servers
  that have been configured in barman

- Added the 'show-server' command that shows the relevant information
  for a given server, including all configuration options

- Added the 'status' command which shows information about the current
  state of a server, including Postgres version, current transaction ID,
  archive command, etc.

- Added the 'check' command which returns 0 if everything Barman needs
  is functioning correctly

- Added the 'list-backup' command that lists all the available backups
  for a given server, including size of the base backup and total size
  of the related WAL segments

- Added the 'show-backup' command that shows the relevant information
  for a given backup, including time of start, size, number of related
  WAL segments and their size, etc.

- Added the 'delete' command which removes a backup from the catalog

- Added the 'list-files' command which lists all the files for a
  single backup

- RPM Package for RHEL 5/6
