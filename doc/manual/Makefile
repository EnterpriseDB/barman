DOCS= \
  barman-manual.en.pdf barman-manual.en.html

MDS = \
	??-*.en.md
  #00-head.en.md \
  #01-intro.en.md \
  #02-before_you_start.en.md \
  #10-design.en.md

all: $(DOCS)

barman-manual.en.pdf: $(MDS) ../images/*.png
	pandoc -o $@ -s -f markdown --toc $(MDS)

barman-manual.en.html: $(MDS) ../images/*.png
	pandoc -o $@ -t html5 -f markdown -s -S --toc $(MDS)

clean:
	    rm -f $(DOCS)

help:
	    @echo "Usage:"
	    @echo "    $$ make"

.PHONY: all clean help 
