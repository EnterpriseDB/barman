# User documentation and man pages

The user documentation and the man pages for Barman are built using Sphinx.
All the docs content and the configuration for Sphinx are found inside the `docs`
directory.

The docs are built using Sphinx via `uv`, which manages the Python environment
and installs all required dependencies automatically.

From the root directory, generate the docs:

* For HTML docs:

```bash
uv run --locked --all-extras --no-dev --group docs make -C docs html
```

* For man pages:

```bash
uv run --locked --all-extras --no-dev --group docs make -C docs man
```

* For PDF docs:

```bash
uv run --locked --all-extras --no-dev --group docs make -C docs latexpdf
```

Once the build finishes, you can read the built documentation:

* For HTML docs: open `docs/_build/html/index.html` with your web browser;
* For man pages: run `man docs/_build/man/barman.1`;
* For PDF docs: open `docs/_build/latex/Barman.pdf` with your PDF reader.
