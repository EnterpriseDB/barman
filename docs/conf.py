# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2023
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

"""
Configuration file for building user docs and man pages for Barman.

This file is executed with the current directory set to its containing dir.

Note that not all possible configuration values are present in this file.

All configuration values have a default; values that are commented out serve to
show the default.

If extensions (or modules to document with :mod:`autodoc`) are in another
directory, add these directories to ``sys.path`` here. If the directory is
relative to the documentation root, use :func:`os.path.abspath` to make it
absolute, like shown here.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(".."))


from barman.version import __version__  # noqa: E402

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
module_dir = os.path.abspath(os.path.join(project_root, "barman"))
excludes = ["tests", "setup.py", "conf"]

release_date = os.getenv("SPHINX_BUILD_DATE")
if release_date:
    release_date = datetime.strptime(release_date, "%b %d, %Y").date()
    today = release_date.strftime("%b %d, %Y")

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = "1.0"

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named "sphinx.ext.*") or your custom
# ones.
extensions = [
    "myst_parser",  # To support Markdown-based docs, Sphinx can use MyST-Parser.
    "sphinx.ext.intersphinx",  # Implicit links to Python official docs
    "sphinx.ext.todo",  # Support for .. todo:: directive
    "sphinx.ext.mathjax",  # Math symbols
    "sphinx_github_style",  # Generate "View on GitHub" for source code
    "sphinxcontrib.apidoc",  # For generating module docs from code
    "sphinx.ext.autodoc",  # For generating module docs from docstrings
]
apidoc_module_dir = module_dir
apidoc_output_dir = "contributor_guide/modules"
apidoc_excluded_paths = excludes
apidoc_separate_modules = True

# Include autodoc for all members, including private ones and the ones that are
# missing a docstring.
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "private-members": True,
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "Barman"
copyright = "© Copyright EnterpriseDB UK Limited 2011-2024"
author = "EnterpriseDB"

# The version info for the project you"re documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = __version__[: __version__.rfind(".")]
# The full version, including alpha/beta/rc tags.
release = __version__

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = "en"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#

html_theme = "pydata_sphinx_theme"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for the theme, see the documentation at
# https://pydata-sphinx-theme.readthedocs.io/en/latest/user_guide/layout.html#references
#
# PyData theme options
html_theme_options = {
    # Top nav
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/EnterpriseDB/barman",
            "icon": "fa-brands fa-github",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/barman",
            "icon": "fa-brands fa-python",
        },
    ],
    # Right side bar
    "show_toc_level": 4,
    "use_edit_page_button": True,
    # Footer
    "footer_start": [],
    "footer_center": [],
    "footer_end": [],
}

html_sidebars = {"**": ["sidebar-nav-bs.html"]}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["images"]

# Replace "source" links with "edit on GitHub" when using rtd theme
html_context = {
    "display_github": True,
    "github_user": "EnterpriseDB",
    "github_repo": "barman",
    "github_version": "master",
    "doc_path": "docs",
    "conf_py_path": "/docs/",
}

# sphinx-github-style options, https://sphinx-github-style.readthedocs.io/en/latest/index.html

# The name of the top-level package.
top_level = "barman"

# The blob to link to on GitHub - any of "head", "last_tag", or "{blob}"
# linkcode_blob = "head"

# The link to your GitHub repository formatted as https://github.com/user/repo
# If not provided, will attempt to create the link from the html_context dict
# linkcode_url = f"https://github.com/{html_context["github_user"]}/" \
#                f"{html_context["github_repo"]}/{html_context["github_version"]}"

# The text to use for the linkcode link
# linkcode_link_text: str = "View on GitHub"

# A linkcode_resolve() function to use for resolving the link target
# linkcode_resolve: types.FunctionType

# -- Options for HTMLHelp output ------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = "Barmandoc"

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # The paper size ("letterpaper" or "a4paper").
    #
    # "papersize": "letterpaper",
    # The font size ("10pt", "11pt" or "12pt").
    #
    # "pointsize": "10pt",
    # Additional stuff for the LaTeX preamble.
    #
    # "preamble": "",
    # Latex figure (float) alignment
    #
    # "figure_align": "htbp",
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    ("index_pdf", "Barman.tex", "Barman Documentation", author, "manual"),
]

# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    ("user_guide/configuration", "barman", "Barman Configurations", [author], 5),
    ("user_guide/commands", "barman", "Barman Commands", [author], 1),
    (
        "user_guide/commands/barman/archive_wal.inc",
        "barman-archive-wal",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/backup.inc",
        "barman-backup",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/check_backup.inc",
        "barman-check-backup",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/check.inc",
        "barman-check",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/config_switch.inc",
        "barman-config-switch",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/config_update.inc",
        "barman-config-update",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/cron.inc",
        "barman-cron",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/delete.inc",
        "barman-delete",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/diagnose.inc",
        "barman-diagnose",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/generate_manifest.inc",
        "barman-generate-manifest",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/get_wal.inc",
        "barman-get-wal",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/keep.inc",
        "barman-keep",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/list_backups.inc",
        "barman-list_backups",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/list_files.inc",
        "barman-list-files",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/list_servers.inc",
        "barman-list-servers",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/lock_directory_cleanup.inc",
        "barman-lock-directory-cleanup",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/put_wal.inc",
        "barman-put-wal",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/rebuild_xlogdb.inc",
        "barman-rebuild-xlogdb",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/receive_wal.inc",
        "barman-receive-wal",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/restore.inc",
        "barman-restore",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/replication_status.inc",
        "barman-replication-status",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/show_backup.inc",
        "barman-show-backup",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/show_servers.inc",
        "barman-show-servers",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/status.inc",
        "barman-status",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/switch_wal.inc",
        "barman-switch-wal",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/switch_xlog.inc",
        "barman-switch-xlog",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/sync_backup.inc",
        "barman-sync-backup",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/sync_info.inc",
        "barman-sync-info",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/sync_wals.inc",
        "barman-sync-wals",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/verify_backup.inc",
        "barman-verify-backup",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman/verify.inc",
        "barman-verify",
        "Barman Sub-Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/backup.inc",
        "barman-cloud-backup",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/backup_delete.inc",
        "barman-cloud-backup-delete",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/backup_show.inc",
        "barman-cloud-backup-show",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/backup_list.inc",
        "barman-cloud-backup-list",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/backup_keep.inc",
        "barman-cloud-backup-keep",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/check_wal_archive.inc",
        "barman-cloud-check-wal-archive",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/restore.inc",
        "barman-cloud-restore",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/wal_archive.inc",
        "barman-cloud-wal-archive",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cloud/wal_restore.inc",
        "barman-cloud-wal-restore",
        "Barman-cloud Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cli/wal_archive.inc",
        "barman-wal-archive",
        "Barman-cli Commands",
        [author],
        1,
    ),
    (
        "user_guide/commands/barman_cli/wal_restore.inc",
        "barman-wal-restore",
        "Barman-cli Commands",
        [author],
        1,
    ),
]

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {"python": ("https://docs.python.org/", None)}

# Remove these pages from index, references, toc trees, etc.
# If the builder is not "html" then add the API docs modules index to pages to be removed.
exclude_from_builder = {
    "latex": [
        "contributor_guide/modules",
    ],
}
# Internal holding list, anything added here will always be excluded
_docs_to_remove = []


def builder_inited(app):
    """Run during Sphinx ``builder-inited`` phase.

    Set a config value to builder name and add module docs to :data:`docs_to_remove`.
    """
    print(f"The builder is: {app.builder.name}")
    app.add_config_value("builder", app.builder.name, "env")

    # Remove pages when builder matches any referenced in exclude_from_builder
    if exclude_from_builder.get(app.builder.name):
        _docs_to_remove.extend(exclude_from_builder[app.builder.name])

    # Remove ".inc.rst" files when not building man pages. Those files only make sense
    # for man builder, because we want both man pages for the entire application as well
    # as separate man pages for each command. When building HTML or PDF, we only need
    # the bigger page, not the split ones.
    if app.builder.name != "man":
        exclude_patterns.append("**/*.inc.rst")


def _to_be_removed(doc):
    """Check if *doc* should not be rendered in the built documentation.

    :param doc: the documentation to be checked.

    :return: ``True`` if *doc* should not be rendered, ``False`` otherwise.
    """
    for remove in _docs_to_remove:
        if doc.startswith(remove):
            return True
    return False


def env_get_outdated(app, env, added, changed, removed):
    """Run during Sphinx ``env-get-outdated`` phase.

    Remove the items listed in :data:`_docs_to_remove` from known pages.
    """
    to_remove = set()
    for doc in env.found_docs:
        if _to_be_removed(doc):
            to_remove.add(doc)
    added.difference_update(to_remove)
    changed.difference_update(to_remove)
    removed.update(to_remove)
    env.project.docnames.difference_update(to_remove)
    return []


def doctree_read(app, doctree):
    """Run during Sphinx ``doctree-read`` phase.

    Remove the items listed in :data:`_docs_to_remove` from the table of contents.
    """
    from sphinx import addnodes

    for toc_tree_node in doctree.traverse(addnodes.toctree):
        for e in toc_tree_node["entries"]:
            if _to_be_removed(str(e[1])):
                toc_tree_node["entries"].remove(e)


def autodoc_skip(app, what, name, obj, would_skip, options):
    """Include autodoc of ``__init__`` methods, which are skipped by default."""
    if name == "__init__":
        return False
    return would_skip


# A possibility to have an own stylesheet, to add new rules or override existing ones
# For the latter case, the CSS specificity of the rules should be higher than the
# default ones
def setup(app):
    """Entry-point when setting up a ``sphinx-build`` execution."""
    if hasattr(app, "add_css_file"):
        app.add_css_file("custom.css")
    else:
        app.add_stylesheet("custom.css")

    # Run extra steps to remove module docs when running with a non-html builder
    app.connect("builder-inited", builder_inited)
    app.connect("env-get-outdated", env_get_outdated)
    app.connect("doctree-read", doctree_read)
    app.connect("autodoc-skip-member", autodoc_skip)
