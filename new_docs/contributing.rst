.. _contributing:

Contributing to Barman
======================

Barman is an open-source project and we welcome contributions from the community.

Follow these guidelines when playing with Barman code and contributing patches to the
project.

Setting up a development installation
-------------------------------------

Writing code
------------

Writing and running tests
-------------------------

Writing and building documentation
----------------------------------

The documentation is written using `reStructuredText language <https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html>`_
and is built with `sphinx-build <https://www.sphinx-doc.org/en/master/man/sphinx-build.html>`_
through a tox environment.

Style guide
^^^^^^^^^^^

Follow these guidelines when writing the documentation.

Tense and voice
"""""""""""""""

For reference and general task-based docs use the "imperative mood". These docs should
be straightforward and conventional. For example:

.. code-block:: rst

    To create a user, run:

    .. code-block:: sql

        CREATE USER my_user;

For tutorials, the docs can be more casual and conversational but must also be
straightforward and clear. For example:

.. code-block:: rst

    In this lab, start with a fresh cluster. Make sure to stop and clean up the cluster
    from the previous labs.

Future and conditional tenses
"""""""""""""""""""""""""""""

Avoid future tense (will) and conditional tenses (would, could, should). These tenses
lack precision and can create passive voice.

Use future tense when an action occurs in the future, for example:

.. code-block:: rst

    This feature will be removed in a future release.

While present tense is strongly preferred, future tense can be useful and accurate in an
"if/then" phrase. For example, it's okay to write:

.. code-block:: rst

    If you perform this action, another action will occur.

The conditional tense is okay only if you explain the conditions and any action to take.
For example, use:

.. code-block:: rst

    A message should appear. If it doesn't, restart the server.

Person
""""""

Use second person (you) when referring to the user. Don't use "the user" which is third
person.

Use first person plural (we) to refer to the authors of the docs. For example, use:

.. code-block:: rst

    We recommend that you restart your server.

Instead of:

.. code-block:: rst

    Developers recommend that you restart your server.

However, don't use first person plural when talking about how the software works or in
an example. For example, use:

.. code-block:: rst

    Next, Barman processes the instruction.

Instead of:

.. code-block:: rst

    Next, we process the instruction.

Line length
"""""""""""

When possible do not overflow 88 characters per line in the source files. In general,
exceptions for this rule are links.

Sentence length
"""""""""""""""

Avoid writing sentences with more than 26 words. Long sentences tend to make the content
complicated.

Contractions
""""""""""""

In keeping with the casual and friendly tone, use contractions. However, use common
contractions (isn't, can't, don't). Don't use contractions that are unclear or difficult
to pronounce (there'll).

Numbers
"""""""

Spell out numbers zero through nine. Use digits for numbers 10 and greater. Spell out
any number that starts a sentence. For this reason, avoid starting a sentence with a
long or complex number.

Dates
"""""

When specifying dates for human readability, use the DD mmm YYYY format with a short
month name in English. Where the date is being used in a column in a table, use a
leading 0 on the day of month for easier alignment, for example, 01 Jan 2024.

When specifying dates as solely numbers, use `ISO8601 <https://www.iso.org/iso-8601-date-and-time-format.html>`_
format; YYYY/MM/DD. This is the internationally accepted, disambiguous format. Use it
where you may expect the date to be read by automated systems.

Capitalization
""""""""""""""

Capitalization rules:

* Use sentence-case for headings (including column headings in tables).
* Capitalize the first letter in each list item except for function and command names
  that are naturally lower case.
* Capitalize link labels to match the case of the topic you're linking to.
* Capitalize proper nouns and match the case of UI features.
* Don't capitalize the words that make up an initialization unless they're part of
  proper noun. For example, single sign-on is not a proper noun even though it's usually
  written as the initialism SSO.

Punctuation
"""""""""""

Punctuation rules:

* Avoid semicolons. Instead, use two sentences.
* Don't join related sentences using a comma. This syntax is incorrect.
* Don't end headings with a period or colon.
* Use periods at the end of list items that are a sentence or that complete a sentence.
  If one item in a list uses a period, use a period for all the items in that list.
* Use the `Oxford (AKA serial) comma <https://en.wikipedia.org/wiki/Serial_comma>`_.

"This" without a noun
"""""""""""""""""""""

Avoid using "this" without a noun following. Doing so can lead to ambiguity. For
example, use:

.. code-block:: rst

    This error happens when…

Instead of:

.. code-block:: rst

    This happens when...

Directing users up and down through a topic
"""""""""""""""""""""""""""""""""""""""""""

Don't use words like "above" and "below" to refer to previous and following sections.
Link to the section instead or use "earlier" or "later".

It also isn't necessary to use the words "the following" to refer to list items. These
words are empty. So, for example, use:

.. code-block:: rst

    The color palette includes:

Instead of:

.. code-block:: rst

    The palette includes the following colors:

Bold (**text**)
"""""""""""""""

Use for UI elements. For example:

.. code-block:: rst

    The output of ``barman show-backup`` includes:

    * **Backup Size**: the size of the backup.
    * **Estimated Cluster Size**: the estimated size of the cluster once the backup is
      restored.

Also for roles and users. For example:

.. code-block:: rst

    Run as **root**:

    .. code-block:: bash

        dnf install barman

Courier (AKA inline code or monospace ``text``)
"""""""""""""""""""""""""""""""""""""""""""""""

Use for parameters, commands, text in configuration files, and file paths. Don't use for
utility names. For example:

.. code-block:: rst

    If you need to type the ``ls`` or ``dd`` command, add a setting to a
    ``configuration=file`` or just refer to ``/etc/passwd``, then this is the font
    treatment to use.

Code blocks
"""""""""""

Use to provide code or configuration samples.

Example for code sample:

.. code-block:: rst

    Execute this query:

    .. code-block:: sql

        SELECT *
        FROM pg_stat_activity;

Example for configuration sample:

.. code-block:: rst

    Create the file ``/etc/barman.conf`` with:

    .. code-block:: ini

        [barman]
        ; System user
        barman_user = barman

        ; Directory of configuration files. Place your sections in separate files with
        ; .conf extension
        ; For example place the 'main' server section in /etc/barman.d/main.conf
        configuration_files_directory = /etc/barman.d

Italics (*text*)
""""""""""""""""

Use for book titles. For example:

.. code-block:: rst

    We recommend you read *PostgreSQL 16 Administration Cookbook*.

Links
"""""

Avoid using the URL as the label. For example, use:

.. code-block:: rst

    For more information about backups in Postgres, see `Backup and Restore <https://www.postgresql.org/docs/current/backup.html>`_.

Instead of:

.. code-block:: rst

    For more information about backups in Postgres, see `https://www.postgresql.org/docs/current/backup.html`_.

Admonitions (notes, warnings, hints, etc.)
""""""""""""""""""""""""""""""""""""""""""

When applicable use `admonitions <https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#admonitions-messages-and-warnings>`_.

For multiple, consecutive admonitions, use separate admonitions.

Tables
""""""

Use `tables <https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#tables>`_
to display structured information in an easy-to-read format.

Lists
"""""

Use `lists <https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#lists-and-quote-like-blocks>`_
to display information in items:

* Numbered (ordered): Use to list information that must appear in order, like tutorial
  steps.
* Bulleted (unordered): Use to list related information in an easy-to-read way.

Use period at the end of each list item.

Images
""""""

Use `images <https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#images>`_
to clarify a topic, but use them only as needed.

Images are put inside the folder ``new_docs/images``.

Cross-reference labels standard
"""""""""""""""""""""""""""""""

When creating cross-reference labels in Sphinx, please follow these guidelines to ensure
consistency and clarity:

1. Use Hyphens: Separate words in labels with a hyphen. For example:

``.. _backup-overview:``

2. Hierarchical Prepending: For each ``.rst`` file, prepend labels with the higher-level
section label, followed by any intermediate sub-section labels. This way, the full
hierarchy is represented in the label.

For example, a file called ``backup.rst`` can have the following label:

``.. _backup:``

Then, any subsequent labels in this file should start with ``backup-``. For a sub-section
labeled ``Overview`` the label would be ``_backup-overview:``. For another sub-section in
``Overview`` the label would be like:

``.. _backup-overview-other-section-under-overview:``

Handling Included Files
~~~~~~~~~~~~~~~~~~~~~~~

If your ``.rst`` file uses the ``.. include::`` directive, evaluate whether the included
files are closely related to the parent document:

* Related Example: In a file ``commands.rst`` with the label:

  ``.. _commands:``

  if you include another file, like ``commands/backup.rst``, which is related, you
  would label the latter as:

  ``.. _commands-backup:``

* Independent Example: If the included section is not directly related, you may treat it
  as an independent section, without the hierarchical label prepending.

Purpose of This Standard
~~~~~~~~~~~~~~~~~~~~~~~~

Following this labeling standard helps us:

* Easily trace the source of cross-references.
* Avoid label duplication.
* Simplify navigation for developers and end-users.

By adhering to these guidelines, we can create clear and maintainable documentation that
enhances usability and understanding.

Building the documentation
^^^^^^^^^^^^^^^^^^^^^^^^^^

You can build the documentation in three different formats:

* HTML: Contains the full documentation.
* PDF: Same as HTML, excluding the section :ref:`contributing`.
* Linux man page: contains only the sections :ref:`configuration` and :ref:`commands`.

The documentation is built through a tox environment named docs.

HTML documentation
""""""""""""""""""

To build the HTML documentation, run:

.. code-block:: bash

    tox -e docs -- html

To view the HTML documentation, open the file ``new_docs/_build/html/index.html`` using
your web browser.

PDF documentation
"""""""""""""""""

To build the PDF documentation, run:

.. code-block:: bash

    tox -e docs -- latexpdf

To view the PDF documentation, open the file ``new_docs/_build/latex/Barman.pdf``
using your PDF reader.

Linux man page
""""""""""""""

To build the Linux man page, run:

.. code-block:: bash

    tox -e docs -- man

To view the Linux man page, run:

.. code-block:: bash

    man new_docs/_build/man/barman.1

Opening a PR
------------

Barman API docs
---------------

Refer to :doc:`Barman code API </modules/modules>` for details about the Barman code API.
