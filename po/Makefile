CATALOGS = $(shell ls *.po)

PYFILES  = $(wildcard ../barman/*.py)
POTFILES  = $(PYFILES)

POTFILES.in:
	echo '[encoding: UTF-8]' > $@
	for file in $(POTFILES); do \
		echo "$${file#../}" ; \
	done >> $@

report:
	@for cat in *.po ; do \
		echo -n "$$cat: "; \
		msgfmt --statistics -o /dev/null $$cat; \
	done    

depend:

all:	POTFILES.in

test:
	for n in $(CATALOGS); do \
	msgfmt -c $$n; \
	done

 