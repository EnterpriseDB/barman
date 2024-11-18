.PHONY: all pdf html clean

PDF = manual.pdf

SPECIAL_HTML = index.html

MULTI_HTML = \
	barman.1.html \
	barman.5.html

SINGLE_HTML = \
	barman-cloud-backup.1.html \
	barman-cloud-backup-delete.1.html \
	barman-cloud-backup-keep.1.html \
	barman-cloud-backup-list.1.html \
	barman-cloud-backup-show.1.html \
	barman-cloud-check-wal-archive.1.html \
	barman-cloud-restore.1.html \
	barman-cloud-wal-archive.1.html \
	barman-cloud-wal-restore.1.html \
	barman-wal-archive.1.html \
	barman-wal-restore.1.html

HTML = $(SPECIAL_HTML) $(MULTI_HTML) $(SINGLE_HTML)

# Detect the pandoc major version (1 or 2)
PANDOC_VERSION = $(shell pandoc --version | awk -F '[ .]+' '/^pandoc/{print $$2; exit}')
ifeq ($(PANDOC_VERSION),1)
	SMART = --smart
	NOSMART_SUFFIX =
else
	SMART =
	NOSMART_SUFFIX = -smart
endif

all : $(HTML) $(PDF)

pdf: $(PDF)

html: $(HTML)

clean:
	rm -f $(PDF) $(HTML)

index.html: manual/??-*.en.md images/*.png
	pandoc -t html5 -f markdown$(NOSMART_SUFFIX) -s --toc --toc-depth 2 --template ./html-templates/template.html --css ./html-templates/template.css -o $@ manual/??-*.en.md
	sed -i 's@\.\./images/@images/@g' $@

manual.pdf: manual/??-*.en.md images/*.png
	cd manual && pandoc -f markdown$(NOSMART_SUFFIX) -s --metadata=datadir:../ --template=../templates/Barman.tex -o ../$@ ??-*.en.md ../templates/default.yaml

$(SINGLE_HTML): %.html: source/%.md images/*.png
	pandoc -t html5 -f markdown$(NOSMART_SUFFIX) -s --toc --toc-depth 2 --template ./html-templates/template.html --css ./html-templates/template.css -o $@ $(<)

%.html: source/%.d/??-*.md images/*.png
	pandoc -t html5 -f markdown$(NOSMART_SUFFIX) -s --toc --toc-depth 2 --template ./html-templates/template.html --css ./html-templates/template.css -o $@ $(<D)/??-*.md
