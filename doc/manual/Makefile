DOCS = barman-manual.en.pdf barman-manual.en.html

MDS = $(sort $(wildcard ??-*.en.md))

# Detect the pandoc major version (1 or 2)
PANDOC_VERSION = $(shell pandoc --version | awk -F '[ .]+' '/^pandoc/{print $$2; exit}')
ifeq ($(PANDOC_VERSION),1)
	SMART = --smart
	NOSMART_SUFFIX =
else
	SMART =
	NOSMART_SUFFIX = -smart
endif


all: $(DOCS)

barman-manual.en.pdf: $(MDS) ../images/*.png
	pandoc -o $@ -s -f markdown --toc $(SMART) $(MDS)

barman-manual.en.html: $(MDS) ../images/*.png
	pandoc -o $@ -s -f markdown --toc $(SMART) -t html5 $(MDS)

clean:
	    rm -f $(DOCS)

help:
	    @echo "Usage:"
	    @echo "    $$ make"

.PHONY: all clean help 
