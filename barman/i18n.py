# Copyright (C) 2013-2018 2ndQuadrant Limited
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

import gettext as gettext_module
import os

localedir = os.path.join(os.path.realpath(__file__ + '/../../share'), 'locale')

if not os.path.exists(localedir):
    localedir = '/usr/share/locale'

build_mo = os.path.realpath(__file__ + '/../../build/mo/')
if os.path.isdir(build_mo):
    localedir = build_mo

print(localedir)
LANGUAGES = [
    # Add languages here
    ('es', 'Spanish'),
]


class Trans:
    """
    The purpose of this class is to store the actual translation function upon
    receiving the first call to that function. After this is done, changes to
    USE_I18N will have no effect to which function is served upon request. If
    your tests rely on changing USE_I18N, you can delete all the functions
    from _trans.__dict__.
    Note that storing the function with setattr will have a noticeable
    performance effect, as access to the function goes the normal path,
    instead of using __getattr__.
    """

    def __getattr__(self, attr):
        lang = os.environ.get("LANG")
        if lang.endswith(".UTF-8"):
            lang = lang.rsplit('.', 1)[0]
        for code, _ in LANGUAGES:
            if code and code == lang:
                break
        else:
            # if es_ES is not supported, try es.
            lang = lang.rsplit('_', 1)[0]
            for code, _ in LANGUAGES:
                if code and code == lang:
                    break
            else:
                lang = 'en'

        translation = gettext_module.translation('barman', localedir=localedir,
                                                 languages=[lang], codeset='utf-8', fallback=True)
        setattr(self, attr, getattr(translation, attr))
        return getattr(translation, attr)


_trans = Trans()

# The Trans class is no more needed, so remove it from the namespace.
del Trans


def gettext(message):
    return _trans.gettext(message)


ugettext = gettext

__all__ = ['ugettext']
