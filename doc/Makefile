MANPAGES=barman.1 barman.5
DOCS=barman-tutorial.en.pdf barman-tutorial.en.html

all: $(MANPAGES) $(DOCS)

barman-tutorial.en.pdf: barman-tutorial.en.md
	pandoc $< -o $@ -s -f markdown

barman-tutorial.en.html: barman-tutorial.en.md
	pandoc -o $@ -t html5 -f markdown -s -S --toc $<

barman.1: barman.1.md
	pandoc -s -t man -o $@ $<

barman.5: barman.5.md
	pandoc -s -t man -o $@ $<

clean:
	    rm -f $(MANPAGES) $(DOCS)

help:
	    @echo "Usage:"
	    @echo "    $$ make"

.PHONY: all clean help 
