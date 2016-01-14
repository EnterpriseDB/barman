# Generate sphinx documentation

Generate barman code documentation using Sphinx autodoc

## Prerequisites

Install the python modules required to build the documentation
by executing, from the root directory of Barman:

``` bash
pip install -r sphinx/requirements.txt
```

## Documentation generation

From the root folder of Barman, launch:

``` bash
sphinx/generate_docs.sh
```


### `generate_docs.sh` options

Is possible to use a different path to the barman source files
directory (default: the current barman source directory) passing it
as argument to the `generate_docs.sh` script.

``` bash
sphinx/generate_docs.sh <path_to_alternative_barman_source_dir>
```

It's also possible to pass the target format (default: `html`)
to the generate_docs.sh script using the -t option followed by
one of the available formats:

*  html       to make standalone HTML files
*  dirhtml    to make HTML files named index.html in directories
*  singlehtml to make a single large HTML file
*  pickle     to make pickle files
*  json       to make JSON files
*  htmlhelp   to make HTML files and a HTML help project
*  qthelp     to make HTML files and a qthelp project
*  devhelp    to make HTML files and a Devhelp project
*  epub       to make an epub
*  latex      to make LaTeX files, you can set PAPER=a4 or PAPER=letter
*  latexpdf   to make LaTeX files and run them through pdflatex
*  text       to make text files
*  man        to make manual pages
*  texinfo    to make Texinfo files
*  info       to make Texinfo files and run them through makeinfo
*  gettext    to make PO message catalogs
*  changes    to make an overview of all changed/added/deprecated items
*  linkcheck  to check all external links for integrity
*  doctest    to run all doctests embedded in the documentation (if enabled)

## Licence

Copyright (C) 2011-2016 2ndQuadrant Italia Srl

Barman is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Barman is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Barman.  If not, see <http://www.gnu.org/licenses/>.
