#!/bin/bash

# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

BASEDIR=$(cd ${0%/*}; pwd )

# modify GEN_MODE. It must be passed like parameter value
GEN_MODE='html'

function die()
{
    echo $@
    exit 1
}

function usage()
{
    echo "Usage: $0 [-h] [-t TARGET] DIR"
    echo 
    echo "use -h for extended help"
    echo
    exit 1
}

function showhelp()
{
    echo "$0 [-h] [-t TARGET] DIR"
    echo
    echo "DIR is the source directory of the barman files"
    echo
    echo "  -h	    	Show this help message"
    echo "  -t TARGET  	Generate documentation using a specific " 
    echo "      		target format (default: HTML)"
    echo
    echo "List of available target formats:"
    echo "  html       to make standalone HTML files"
    echo "  dirhtml    to make HTML files named index.html in directories"
    echo "  singlehtml to make a single large HTML file"
    echo "  pickle     to make pickle files"
    echo "  json       to make JSON files"
    echo "  htmlhelp   to make HTML files and a HTML help project"
    echo "  qthelp     to make HTML files and a qthelp project"
    echo "  devhelp    to make HTML files and a Devhelp project"
    echo "  epub       to make an epub"
    echo "  latex      to make LaTeX files, you can set PAPER=a4 or PAPER=letter"
    echo "  latexpdf   to make LaTeX files and run them through pdflatex"
    echo "  text       to make text files"
    echo "  man        to make manual pages"
    echo "  texinfo    to make Texinfo files"
    echo "  info       to make Texinfo files and run them through makeinfo"
    echo "  gettext    to make PO message catalogs"
    echo "  changes    to make an overview of all changed/added/deprecated items"
    echo "  linkcheck  to check all external links for integrity"
    echo "  doctest    to run all doctests embedded in the documentation (if enabled)"
    echo
}

RED='\033[1;31m'
RSET='\033[0m'

function red()
{
    printf "${RED}${1}${RSET}\n"
}


# if -h is the parameter it shows help
# if -t expect for a target
while getopts ht: OPT; 
do
    case "$OPT" in
        t)
	    GEN_MODE=${OPTARG}; shift 2;;
        --)
	    shift; break;;
        h|*)
	    showhelp; exit 1;;
    esac
shift;
done

if [[ $# -gt 2 ]]
then
    showhelp
    exit 1
fi

if [[ $# -eq 0 ]] ; then
    BARMAN_DIR=$(cd "$BASEDIR/.."; pwd)
else
    BARMAN_DIR=$(cd "$1"; pwd)
fi

[[ "${BARMAN_DIR}" = "." ]] && die 'Input directory . is not supported!'
[[ ! -d "${BARMAN_DIR}" ]] && die 'Input directory does not exists!'

export BARMAN_DIR
cd "${BASEDIR}"

# Cleans the build directory
red "Cleaning the Build directory..."
make clean

red "Removing all generated files..."
rm `ls "${BASEDIR}"/docs/*.rst | grep -v 'index.rst$'`

# Generates automatically modules doc
red "Generating documentation from modules..."
sphinx-apidoc -P -e -T -M -o docs "${BARMAN_DIR}"
# Invokes html generation
red "Generating ${GEN_MODE}"
make ${GEN_MODE}

red "DONE!!"
