all: barman.1 barman.5 barman-tutorial.en.pdf

CMD=a2x
OPTS=-d manpage -f manpage

barman-tutorial.en.pdf: barman-tutorial.en.md
	    pandoc $< -o $@

barman.1: barman.1.asciidoc
	    $(CMD) $(OPTS) $<

barman.5: barman.5.asciidoc
	    $(CMD) $(OPTS) $<

clean:
	    rm -f barman.1 barman.5 barman-tutorial.en.pdf

help:
	    @echo "Usage:"
	    @echo "    $$ make"

.PHONY: all clean help 
