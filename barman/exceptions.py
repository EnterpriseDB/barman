# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2025
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


class BarmanException(Exception):
    """
    The base class of all other barman exceptions
    """


class ConfigurationException(BarmanException):
    """
    Base exception for all the Configuration errors
    """


class CommandException(BarmanException):
    """
    Base exception for all the errors related to
    the execution of a Command.
    """


class CompressionException(BarmanException):
    """
    Base exception for all the errors related to
    the execution of a compression action.
    """


class PostgresException(BarmanException):
    """
    Base exception for all the errors related to PostgreSQL.
    """


class BackupException(BarmanException):
    """
    Base exception for all the errors related to the execution of a backup.
    """


class WALFileException(BarmanException):
    """
    Base exception for all the errors related to WAL files.
    """

    def __str__(self):
        """
        Human readable string representation
        """
        return "%s:%s" % (self.__class__.__name__, self.args[0] if self.args else None)


class HookScriptException(BarmanException):
    """
    Base exception for all the errors related to Hook Script execution.
    """


class LockFileException(BarmanException):
    """
    Base exception for lock related errors
    """


class SyncException(BarmanException):
    """
    Base Exception for synchronisation functions
    """


class DuplicateWalFile(WALFileException):
    """
    A duplicate WAL file has been found
    """


class MatchingDuplicateWalFile(DuplicateWalFile):
    """
    A duplicate WAL file has been found, but it's identical to the one we
    already have.
    """


class SshCommandException(CommandException):
    """
    Error parsing ssh_command parameter
    """


class UnknownBackupIdException(BackupException):
    """
    The searched backup_id doesn't exists
    """


class BackupInfoBadInitialisation(BackupException):
    """
    Exception for a bad initialization error
    """


class BackupPreconditionException(BackupException):
    """
    Exception for a backup precondition not being met
    """


class SnapshotBackupException(BackupException):
    """
    Exception for snapshot backups
    """


class SnapshotInstanceNotFoundException(SnapshotBackupException):
    """
    Raised when the VM instance related to a snapshot backup cannot be found
    """


class SyncError(SyncException):
    """
    Synchronisation error
    """


class SyncNothingToDo(SyncException):
    """
    Nothing to do during sync operations
    """


class SyncToBeDeleted(SyncException):
    """
    An incomplete backup is to be deleted
    """


class CommandFailedException(CommandException):
    """
    Exception representing a failed command
    """


class CommandMaxRetryExceeded(CommandFailedException):
    """
    A command with retry_times > 0 has exceeded the number of available retry
    """


class RsyncListFilesFailure(CommandException):
    """
    Failure parsing the output of a "rsync --list-only" command
    """


class DataTransferFailure(CommandException):
    """
    Used to pass failure details from a data transfer Command
    """

    @classmethod
    def from_command_error(cls, cmd, e, msg):
        """
        This method build a DataTransferFailure exception and report the
        provided message to the user (both console and log file) along with
        the output of the failed command.

        :param str cmd: The command that failed the transfer
        :param CommandFailedException e: The exception we are handling
        :param str msg: a descriptive message on what we are trying to do
        :return DataTransferFailure: will contain the message provided in msg
        """
        try:
            details = msg
            details += "\n%s error:\n" % cmd
            details += e.args[0]["out"]
            details += e.args[0]["err"]
            return cls(details)
        except (TypeError, NameError):
            # If it is not a dictionary just convert it to a string
            from barman.utils import force_str

            return cls(force_str(e.args))


class CompressionIncompatibility(CompressionException):
    """
    Exception for compression incompatibility
    """


class FileNotFoundException(CompressionException):
    """
    Exception for file not found in archive
    """


class FsOperationFailed(CommandException):
    """
    Exception which represents a failed execution of a command on FS
    """


class LockFileBusy(LockFileException):
    """
    Raised when a lock file is not free
    """


class LockFilePermissionDenied(LockFileException):
    """
    Raised when a lock file is not accessible
    """


class LockFileParsingError(LockFileException):
    """
    Raised when the content of the lockfile is unexpected
    """


class ConninfoException(ConfigurationException):
    """
    Error for missing or failed parsing of the conninfo parameter (DSN)
    """


class PostgresConnectionError(PostgresException):
    """
    Error connecting to the PostgreSQL server
    """

    def __str__(self):
        # Returns the first line
        if self.args and self.args[0]:
            from barman.utils import force_str

            return force_str(self.args[0]).splitlines()[0].strip()
        else:
            return ""


class PostgresConnectionLost(PostgresException):
    """
    The Postgres connection was lost during an execution
    """


class PostgresAppNameError(PostgresConnectionError):
    """
    Error setting application name with PostgreSQL server
    """


class PostgresSuperuserRequired(PostgresException):
    """
    Superuser access is required
    """


class BackupFunctionsAccessRequired(PostgresException):
    """
    Superuser or access to backup functions is required
    """


class PostgresCheckpointPrivilegesRequired(PostgresException):
    """
    Superuser or role 'pg_checkpoint' is required
    """


class PostgresIsInRecovery(PostgresException):
    """
    PostgreSQL is in recovery, so no write operations are allowed
    """


class PostgresUnsupportedFeature(PostgresException):
    """
    Unsupported feature
    """


class PostgresObsoleteFeature(PostgresException):
    """
    Obsolete feature, i.e. one which has been deprecated and since
    removed.
    """


class PostgresDuplicateReplicationSlot(PostgresException):
    """
    The creation of a physical replication slot failed because
    the slot already exists
    """


class PostgresReplicationSlotsFull(PostgresException):
    """
    The creation of a physical replication slot failed because
    the all the replication slots have been taken
    """


class PostgresReplicationSlotInUse(PostgresException):
    """
    The drop of a physical replication slot failed because
    the replication slots is in use
    """


class PostgresInvalidReplicationSlot(PostgresException):
    """
    Exception representing a failure during the deletion of a non
    existent replication slot
    """


class TimeoutError(CommandException):
    """
    A timeout occurred.
    """


class ArchiverFailure(WALFileException):
    """
    Exception representing a failure during the execution
    of the archive process
    """


class BadXlogSegmentName(WALFileException):
    """
    Exception for a bad xlog name
    """


class BadXlogPrefix(WALFileException):
    """
    Exception for a bad xlog prefix
    """


class BadHistoryFileContents(WALFileException):
    """
    Exception for a corrupted history file
    """


class AbortedRetryHookScript(HookScriptException):
    """
    Exception for handling abort of retry hook scripts
    """

    def __init__(self, hook):
        """
        Initialise the exception with hook script info
        """
        self.hook = hook

    def __str__(self):
        """
        String representation
        """
        return "Abort '%s_%s' retry hook script (%s, exit code: %d)" % (
            self.hook.phase,
            self.hook.name,
            self.hook.script,
            self.hook.exit_status,
        )


class RecoveryException(BarmanException):
    """
    Exception for a recovery error
    """


class RecoveryPreconditionException(RecoveryException):
    """
    Exception for a recovery precondition not being met
    """


class RecoveryTargetActionException(RecoveryException):
    """
    Exception for a wrong recovery target action
    """


class RecoveryStandbyModeException(RecoveryException):
    """
    Exception for a wrong recovery standby mode
    """


class RecoveryInvalidTargetException(RecoveryException):
    """
    Exception for a wrong recovery target
    """


class UnrecoverableHookScriptError(BarmanException):
    """
    Exception for hook script errors which mean the script should not be retried.
    """


class ArchivalBackupException(BarmanException):
    """
    Exception for errors concerning archival backups.
    """


class WalArchiveContentError(BarmanException):
    """
    Exception raised when unexpected content is detected in the WAL archive.
    """


class InvalidRetentionPolicy(BarmanException):
    """
    Exception raised when a retention policy cannot be parsed.
    """


class BackupManifestException(BarmanException):
    """
    Exception raised when there is a problem with the backup manifest.
    """


class EncryptionCommandException(CommandFailedException):
    """
    Exception representing a failed encryption command.
    """
