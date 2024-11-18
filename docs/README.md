# User documentation and man pages

The user documentation and the man pages for Barman are built using Sphinx.
All the docs content and the configuration for Sphinx are found inside the `docs`
directory.

There is an automation through tox to build the docs, which takes care of
installing all the required Python modules.

From the root directory, install the dependencies of tox:

```bash
pip install -r requirements-tox.txt
```

Then, to generate the docs you can just use the tox environment `docs`:

* For HTML docs:

```bash
tox -e docs -- html
```

* For man pages:

```bash
tox -e docs -- man
```

* For PDF docs:

```bash
tox -e docs -- latexpdf
```

Once the build finishes, you can read the built documentation:

* For HTML docs: open `docs/_build/html/index.html` with your web browser;
* For man pages: run `man docs/_build/man/barman.1`;
* For PDF docs: open `docs/_build/latex/Barman.pdf` with your PDF reader.
