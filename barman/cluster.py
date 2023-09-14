import logging

import barman.config

from barman import output
from barman.config import ServerConfig
from barman.server import CheckOutputStrategy, CheckStrategy, Server
from barman.utils import timeout

_logger = logging.getLogger(__name__)


class Cluster(object):
    """A PostgreSQL cluster consisting of one or more Servers."""

    # the strategy for the management of the results of the various checks
    __default_check_strategy = CheckOutputStrategy()

    def __init__(self, config):
        self.raw_config = config
        if config.cluster_hosts is None:
            # This cluster only has one server, so all nodes are the same
            self.backup_server = Server(config)
            self.wal_server = self.backup_server
            self.primary_server = self.backup_server
        else:
            backup_host = config.cluster_hosts[config.cluster_backup_source]
            primary_host = config.cluster_hosts[config.cluster_primary]
            # This could one day come from cluster_wal_source but for now it's always
            # the primary
            wal_host = primary_host
            self.backup_server = self._create_server_for_host(config, backup_host)
            # Set primary_conninfo if we are not backing up from the primary
            if backup_host != primary_host:
                self.backup_server.config.primary_conninfo = (
                    config.conninfo + f" host={primary_host}"
                )

            self.wal_server = self._create_server_for_host(config, wal_host)
            self.primary_server = self._create_server_for_host(config, primary_host)

    def _create_server_for_host(self, config, host):
        server_config = ServerConfig(barman.__config__, config.name)
        server_config.conninfo += f" host={host}"
        server_config.ssh_command += host
        server_config.streaming_conninfo += f" host={host}"
        # If we are the backup source host and we are not the primary, set primary_conninfo
        if (
            config.cluster_backup_source != config.cluster_primary
            and host == config.cluster_hosts[config.cluster_backup_source]
        ):
            server_config.primary_conninfo = (
                config.conninfo
                + f" host={config.cluster_hosts[config.cluster_primary]}"
            )
        return Server(server_config)

    @property
    def config(self):
        return self.raw_config

    def close(self):
        self.backup_server.close()
        self.wal_server.close()
        self.primary_server.close()

    # Functions which must be carried out on the backup source
    def check(self, check_strategy=__default_check_strategy):
        # TODO this needs to be cleverer in a clustered scenario
        # What do we do differently to check the WAL server and the
        # primary server?
        # How do we communicate this to users?
        return self._check(check_strategy)

    def cron(self, wals=True, retention_policies=True, keep_descriptors=True):
        return self.backup_server.cron(wals, retention_policies, keep_descriptors)

    @property
    def passive_node(self):
        return self.backup_server.passive_node

    def backup(self, wait=False, wait_timeout=None, backup_name=None):
        # The 'backup' command is not available on a passive node.
        # We assume that if we get here the node is not passive
        assert not self.passive_node

        # Default strategy for check in backup is CheckStrategy
        # This strategy does not print any output - it only logs checks
        strategy = CheckStrategy()
        self.check(strategy)
        if strategy.has_error:
            output.error(
                "Impossible to start the backup. Check the log "
                "for more details, or run 'barman check %s'" % self.config.name
            )
            return
        return self.backup_server.backup(wait, wait_timeout, backup_name)

    # Functions which must be carried out on the WAL streaming source
    def create_physical_repslot(self):
        return self.wal_server.create_physical_repslot()

    def receive_wal(self, reset=False):
        return self.wal_server.receive_wal(reset)

    def drop_repslot(self):
        return self.wal_server.drop_repslot()

    def show(self):
        # This retrieves the server status including: System ID, PostgreSQL version and
        # PGDATA directory, along with local config data.
        # Should we get this from the primary? Or the backup source? Or show the details
        # for each?
        return self.primary_server.show()

    # Functions which must be carried out on the primary
    def switch_wal(self, force=False, archive=None, archive_timeout=None):
        return self.primary_server.switch_wal(force, archive, archive_timeout)

    # Functions which don't contact PostgreSQL so can be carried out on any Server
    def kill(self, task, fail_if_not_present=True):
        return self.backup_server.kill(task, fail_if_not_present)

    def archive_wal(self, verbose=True):
        return self.backup_server.archive_wal(verbose)

    # Functions which used to live in the Server but live in the Cluster now
    def _check(self, check_strategy):
        # Only one server so just do the regular check
        if self.backup_server == self.wal_server == self.primary_server:
            return self.backup_server.check(check_strategy)
        try:
            with timeout(self.raw_config.check_timeout):
                # Backup server checks
                # Check WAL archive
                self.backup_server.check_archive(check_strategy)
                # Check regular PostgreSQL connections
                self.backup_server.check_postgres(
                    check_strategy, skip_replication_check=True
                )
                # Check barman directories from barman configuration
                self.backup_server.check_directories(check_strategy)
                # Check retention policies
                self.backup_server.check_retention_policy_settings(check_strategy)
                # Check for backup validity
                self.backup_server.check_backup_validity(check_strategy)
                # Check WAL archiving is happening
                self.backup_server.check_wal_validity(check_strategy)
                # Executes the backup manager set of checks
                self.backup_server.backup_manager.check(check_strategy)
                # Check if the msg_list of the server
                # contains messages and output eventual failures
                self.backup_server.check_configuration(check_strategy)
                # Check the system Id coherence between
                # streaming and normal connections
                self.backup_server.check_identity(check_strategy)
                # Executes check() for every archiver, passing
                # remote status information for efficiency
                for archiver in self.backup_server.archivers:
                    archiver.check(check_strategy)

                # Check archiver errors
                self.backup_server.check_archiver_errors(check_strategy)
                if self.wal_server != self.backup_server:
                    # WAL source (aka primary) checks
                    self.wal_server.check_postgres(check_strategy)
        except TimeoutError:
            # The check timed out.
            # Add a failed entry to the check strategy for this.
            _logger.debug(
                "Check command timed out executing '%s' check"
                % check_strategy.running_check
            )
            check_strategy.result(
                self.raw_config.name,
                False,
                hint="barman check command timed out",
                check="check timeout",
            )
