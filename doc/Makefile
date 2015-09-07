MANPAGES=barman.1 barman.5
DOCS=barman-tutorial.en.pdf barman-tutorial.en.html

all: $(MANPAGES) $(DOCS)

CMD=a2x
OPTS=-d manpage -f manpage

barman-tutorial.en.pdf: barman-tutorial.en.md
	    pandoc $< -o $@ -s -f markdown

barman-tutorial.en.html: barman-tutorial.en.md
	pandoc -o $@ -t html5 -f markdown -s -S --toc $<

barman.1: barman.1.asciidoc
	    $(CMD) $(OPTS) $<

barman.5: barman.5.asciidoc
	    $(CMD) $(OPTS) $<

clean:
	    rm -f $(MANPAGES) $(DOCS)

help:
	    @echo "Usage:"
	    @echo "    $$ make"

.PHONY: all clean help 
