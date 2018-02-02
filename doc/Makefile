MANPAGES=barman.1 barman.5
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
