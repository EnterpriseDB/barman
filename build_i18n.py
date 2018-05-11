import sys
import os
import glob

try:
    from setuptools import setup
    from setuptools import Command
except ImportError:
    from distutils.core import setup
    from distutils.cmd import Command

PO_DIR="po"
DOMAIN="barman"

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

        data_files = self.distribution.data_files
        if data_files is None:
            # in case not data_files are defined in setup.py
            self.distribution.data_files = data_files = []

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
        cmd = ["intltool-update", (self.merge_po and "-r" or "-p"), "-g", DOMAIN]
        wd = os.getcwd()
        os.chdir(PO_DIR)
        self.spawn(cmd)
        os.chdir(wd)
        max_po_mtime = 0
        for po_file in glob.glob("%s/*.po" % PO_DIR):
            lang = os.path.basename(po_file[:-3])
            if selected_languages and not lang in selected_languages:
                continue
            mo_dir =  os.path.join("build", "mo", lang, "LC_MESSAGES")
            mo_file = os.path.join(mo_dir, "%s.mo" % DOMAIN)
            if not os.path.exists(mo_dir):
                os.makedirs(mo_dir)
            cmd = ["msgfmt", po_file, "-o", mo_file]
            po_mtime = os.path.getmtime(po_file)
            mo_mtime = os.path.exists(mo_file) and os.path.getmtime(mo_file) or 0
            if po_mtime > max_po_mtime:
                max_po_mtime = po_mtime
            if po_mtime > mo_mtime:
                self.spawn(cmd)


def polist():
    po_files = []
    for po_file in glob.glob("%s/*.po" % PO_DIR):
        lang = os.path.basename(po_file[:-3])
        mo_dir =  os.path.join("build", "mo", lang, "LC_MESSAGES")
        mo_file = os.path.join(mo_dir, "%s.mo" % DOMAIN)

        targetpath = os.path.join("share/locale", lang, "LC_MESSAGES")
        po_files.append((targetpath, (mo_file,)))
    return po_files