MANPAGES=barman.1 barman.5
DOCS= \
			barman-quickstart.en.pdf barman-quickstart.en.html

SUBDIRS=manual

.PHONY: all clean help subdirs $(SUBDIRS)

all: $(MANPAGES) $(DOCS) $(SUBDIRS)

barman-quickstart.en.pdf: barman-quickstart.en.md images/*.png
	pandoc $< -o $@ -s -f markdown

barman-quickstart.en.html: barman-quickstart.en.md images/*.png
	pandoc -o $@ -t html5 -f markdown -s -S --toc $<

barman.1: barman.1.md
	pandoc -s -t man -o $@ $<

barman.5: barman.5.md
	pandoc -s -t man -o $@ $<

clean:
	    rm -f $(MANPAGES) $(DOCS)
			for dir in $(SUBDIRS); do \
				$(MAKE) -C $$dir clean; \
			done

help:
	    @echo "Usage:"
	    @echo "    $$ make"

subdirs: $(SUBDIRS)

$(SUBDIRS):
			$(MAKE) -C $@

