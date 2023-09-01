from barman.server import CheckOutputStrategy, Server


class Cluster(object):
    """A PostgreSQL cluster consisting of one or more Servers."""

    # the strategy for the management of the results of the various checks
    __default_check_strategy = CheckOutputStrategy()

    def __init__(self, config):
        self.backup_server = Server(config)
        self.wal_server = self.backup_server
        self.primary_server = self.backup_server

    @property
    def config(self):
        return self.backup_server.config

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
        return self.backup_server.check(check_strategy)

    def cron(self, wals=True, retention_policies=True, keep_descriptors=True):
        return self.backup_server.cron(wals, retention_policies, keep_descriptors)

    @property
    def passive_node(self):
        return self.backup_server.passive_node

    def backup(self, wait=False, wait_timeout=None, backup_name=None):
        return self.backup_server.backup(wait, wait_timeout, backup_name)

    # Functions which must be carried out on the WAL streaming source
    def create_physical_repslot(self):
        return self.wal_server.create_physical_repslot()

    def receive_wal(self, reset=False):
        return self.wal_server.receive_wal(reset)

    def drop_repslot(self):
        return self.wal_server.drop_repslot()

    # Functions which must be carried out on the primary
    def switch_wal(self, force=False, archive=None, archive_timeout=None):
        return self.primary_server.switch_wal(force, archive, archive_timeout)

    # Functions which don't contact PostgreSQL so can be carried out on any Server
    def kill(self, task, fail_if_not_present=True):
        return self.backup_server.kill(task, fail_if_not_present)

    def archive_wal(self, verbose=True):
        return self.backup_server.archive_wal(verbose)
