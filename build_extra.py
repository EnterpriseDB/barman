import distutils
import glob
import os
import os.path
import re
import shutil
import sys
import distutils.command.build as _build
import distutils.command.clean as _clean

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
            return self.i18n == "True" or \
                   ("build_i18n" in self.distribution.cmdclass and \
                    self.i18n != "False")

        _build.build.finalize_options(self)
        self.sub_commands.append(("build_i18n", has_i18n))

class clean(_clean.clean):
    def run(self):
        _clean.clean.run(self)

        for dir in ['build', 'dist', 'barman.egg-info', '.eggs', '.cache']:
            if os.path.exists(dir):
                shutil.rmtree(dir)