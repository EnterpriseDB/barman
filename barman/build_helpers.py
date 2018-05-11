# Copyright (C) 2011-2018 2ndQuadrant Limited
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

import distutils.command.build as _build
import distutils.command.clean as _clean
import glob
import os
import shutil
from distutils.cmd import Command

PO_DIR = "po"
DOMAIN = "barman"


class build_i18n(Command):
    description = "integrate the gettext framework"

    user_options = [('merge-po', 'm', 'merge po files against template'),
                    ('bug-contact=', None, 'contact address for msgid bugs')]

    boolean_options = ['merge-po']

    def initialize_options(self):
        self.merge_po = False
        self.bug_contact = None

    def finalize_options(self):
        pass

    def run(self):
        """
        Update the language files, generate mo files and add them
        to the to be installed files
        """
        if not os.path.isdir(PO_DIR):
            return

        if self.bug_contact is not None:
            os.environ["XGETTEXT_ARGS"] = "--msgid-bugs-address=%s " % \
                                          self.bug_contact

        # Print a warning if there is a Makefile that would overwrite our
        # values
        if os.path.exists("%s/Makefile" % PO_DIR):
            self.announce("""
WARNING: Intltool will use the values specified from the
         existing po/Makefile in favor of the vaules
         from setup.cfg.
         Remove the Makefile to avoid problems.""")

        # If there is a po/LINGUAS file, or the LINGUAS environment variable
        # is set, only compile the languages listed there.
        selected_languages = None
        linguas_file = os.path.join(PO_DIR, "LINGUAS")
        if os.path.isfile(linguas_file):
            selected_languages = open(linguas_file).read().split()
        if "LINGUAS" in os.environ:
            selected_languages = os.environ["LINGUAS"].split()

        # Update po(t) files and print a report
        # We have to change the working dir to the po dir for intltool
        cmd = ["intltool-update", (self.merge_po and "-r" or "-p"), "-g",
               DOMAIN]
        wd = os.getcwd()
        os.chdir(PO_DIR)
        self.spawn(cmd)
        os.chdir(wd)
        max_po_mtime = 0
        for po_file in glob.glob("%s/*.po" % PO_DIR):
            lang = os.path.basename(po_file[:-3])
            if selected_languages and lang not in selected_languages:
                continue
            mo_dir = os.path.join("build", "mo", lang, "LC_MESSAGES")
            mo_file = os.path.join(mo_dir, "%s.mo" % DOMAIN)
            if not os.path.exists(mo_dir):
                os.makedirs(mo_dir)
            cmd = ["msgfmt", po_file, "-o", mo_file]
            po_mtime = os.path.getmtime(po_file)
            mo_mtime = os.path.exists(mo_file) and os.path.getmtime(
                mo_file) or 0
            if po_mtime > max_po_mtime:
                max_po_mtime = po_mtime
            if po_mtime > mo_mtime:
                self.spawn(cmd)


def molist():
    po_files = []
    for po_file in glob.glob("%s/*.po" % PO_DIR):
        lang = os.path.basename(po_file[:-3])
        mo_dir = os.path.join("build", "mo", lang, "LC_MESSAGES")
        mo_file = os.path.join(mo_dir, "%s.mo" % DOMAIN)

        targetpath = os.path.join("share/locale", lang, "LC_MESSAGES")
        po_files.append((targetpath, (mo_file,)))
    return po_files


class build_extra(_build.build):
    """Adds the extra commands to the build target. This class should be used
       with the core distutils"""

    def __init__(self, dist):
        _build.build.__init__(self, dist)

        self.user_options.extend([("i18n", None, "use the localisation")])

    def initialize_options(self):
        _build.build.initialize_options(self)
        self.i18n = False

    def finalize_options(self):
        def has_i18n(command):
            return \
                self.i18n == "True" or \
                ("build_i18n" in self.distribution.cmdclass and
                    self.i18n != "False")

        _build.build.finalize_options(self)
        self.sub_commands.append(("build_i18n", has_i18n))


class clean(_clean.clean):
    def run(self):
        _clean.clean.run(self)

        for _dir in ['build', 'dist', 'barman.egg-info', '.eggs', '.cache']:
            if os.path.exists(_dir):
                shutil.rmtree(_dir)
