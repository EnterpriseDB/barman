import logging
import os

from abc import ABCMeta, abstractmethod
from enum import Enum

from barman import xlog
from barman.compression import CompressionManager
from barman.utils import with_metaclass

_logger = logging.getLogger(__name__)


class StorageTier(with_metaclass(ABCMeta)):
    """
    Abstract base class representing a Barman storage tier.
    """

    @abstractmethod
    def get_wal_infos(self):
        """
        Returns ordered list of WalFileInfo objects for all WALs in this storage tier.
        """


class StorageTierRaw(StorageTier):
    """
    Storage tier for local unprocessed backups.
    """

    def __init__(self, server_config, path):
        self.server = server_config.name
        self.path = path
        self.compression_manager = CompressionManager(server_config, self.path)

    def is_wal_archive_empty(self):
        try:
            next(self.get_wal_infos())
            return False
        except StopIteration:
            return True

    def get_wal_infos(self):
        comp_manager = self.compression_manager
        wal_count = label_count = history_count = 0
        for name in sorted(os.listdir(self.path)):
            # ignore the xlogdb and its lockfile
            # TODO hardcoded for now to avoid a circular dep but should do properly
            if name.startswith("xlog.db"):
                continue
            fullname = os.path.join(self.path, name)
            if os.path.isdir(fullname):
                # all relevant files are in subdirectories
                hash_dir = fullname
                for wal_name in sorted(os.listdir(hash_dir)):
                    fullname = os.path.join(hash_dir, wal_name)
                    if os.path.isdir(fullname):
                        _logger.warning(
                            "unexpected directory " "rebuilding the wal database: %s",
                            fullname,
                        )
                    else:
                        if xlog.is_wal_file(fullname):
                            wal_count += 1
                        elif xlog.is_backup_file(fullname):
                            label_count += 1
                        elif fullname.endswith(".tmp"):
                            _logger.warning(
                                "temporary file found "
                                "rebuilding the wal database: %s",
                                fullname,
                            )
                            continue
                        else:
                            _logger.warning(
                                "unexpected file " "rebuilding the wal database: %s",
                                fullname,
                            )
                            continue
                        yield comp_manager.get_wal_file_info(fullname)
            else:
                # only history files are here
                if xlog.is_history_file(fullname):
                    history_count += 1
                    yield comp_manager.get_wal_file_info(fullname)
                else:
                    _logger.warning(
                        "unexpected file rebuilding the wal database: %s",
                        fullname,
                    )


class Tier(Enum):
    RAW = StorageTierRaw


def initialize_tiers(server_config):
    """
    Check the config and initialise the necessary tiers, returning them as a dict.

    Currently there is only one tier but this will change.
    """
    return {
        Tier.RAW: Tier.RAW.value(
            server_config,
            server_config.wals_directory,
        )
    }
