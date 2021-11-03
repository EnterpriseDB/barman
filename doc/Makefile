MANPAGES=barman.1 barman.5 \
		barman-wal-archive.1 barman-wal-restore.1 \
    barman-cloud-backup.1 \
    barman-cloud-backup-delete.1 \
    barman-cloud-backup-keep.1 \
    barman-cloud-backup-list.1 \
    barman-cloud-check-wal-archive.1 \
    barman-cloud-wal-archive.1 \
    barman-cloud-restore.1 \
    barman-cloud-wal-restore.1

SUBDIRS=manual

# Detect the pandoc major version (1 or 2)
PANDOC_VERSION = $(shell pandoc --version | awk -F '[ .]+' '/^pandoc/{print $$2; exit}')
ifeq ($(PANDOC_VERSION),1)
	SMART = --smart
	NOSMART_SUFFIX =
else
	SMART =
	NOSMART_SUFFIX = -smart
endif

all: $(MANPAGES) $(SUBDIRS)

barman.1: $(sort $(wildcard barman.1.d/??-*.md))
	pandoc -s -f markdown$(NOSMART_SUFFIX) -t man -o $@ $^

barman.5: $(sort $(wildcard barman.5.d/??-*.md))
	pandoc -s -f markdown$(NOSMART_SUFFIX) -t man -o $@ $^

barman-wal-archive.1: barman-wal-archive.1.md
	pandoc -s -f markdown$(NOSMART_SUFFIX) -t man -o $@ $<

barman-wal-restore.1: barman-wal-restore.1.md
	pandoc -s -f markdown$(NOSMART_SUFFIX) -t man -o $@ $<

barman-cloud-backup.1: barman-cloud-backup.1.md
	pandoc -s -f markdown$(nosmart_suffix) -t man -o $@ $<

barman-cloud-backup-delete.1: barman-cloud-backup-delete.1.md
	pandoc -s -f markdown$(nosmart_suffix) -t man -o $@ $<

barman-cloud-backup-keep.1: barman-cloud-backup-keep.1.md
	pandoc -s -f markdown$(nosmart_suffix) -t man -o $@ $<

barman-cloud-backup-list.1: barman-cloud-backup-list.1.md
	pandoc -s -f markdown$(nosmart_suffix) -t man -o $@ $<

barman-cloud-check-wal-archive.1: barman-cloud-check-wal-archive.1.md
	pandoc -s -f markdown$(nosmart_suffix) -t man -o $@ $<

barman-cloud-restore.1: barman-cloud-restore.1.md
	pandoc -s -f markdown$(nosmart_suffix) -t man -o $@ $<

barman-cloud-wal-archive.1: barman-cloud-wal-archive.1.md
	pandoc -s -f markdown$(nosmart_suffix) -t man -o $@ $<

barman-cloud-wal-restore.1: barman-cloud-wal-restore.1.md
	pandoc -s -f markdown$(nosmart_suffix) -t man -o $@ $<

clean:
	rm -f $(MANPAGES)
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done

help:
	@echo "Usage:"
	@echo "    $$ make"

subdirs: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

.PHONY: all clean help subdirs $(SUBDIRS)
