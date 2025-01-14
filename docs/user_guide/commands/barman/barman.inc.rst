.. _commands-barman:

``barman``
----------

Synopsis
""""""""

.. code-block:: text
    
    barman
        [ { -c | --config } CONFIG ]
        [ { --color | --colour } { never | always | auto } ]
        [ { -d | --debug } ]
        [ { -f | --format } { json | console } ]
        [ { -h | --help } ]
        [ --log-level { NOTSET | DEBUG | INFO | WARNING | ERROR | CRITICAL } ]
        [ { -q | --quiet } ]
        [ { -v | --version } ]
        [ SUBCOMMAND ]

.. note::

    This is the syntax for the synopsis:

    * Options between square brackets are optional.
    * Options between curly brackets represent a choose one of set operation.
    * Options with ``[ ... ]`` can be specified multiple times.
    * Things written in uppercase represent a literal that should be given a value to.

    We will use this same syntax when describing ``barman`` sub-commands in the
    following sections.
    
    Also, when describing sub-commands in the following sections, the commands'
    synopsis should be seen as a replacement for the ``SUBCOMMAND``.

Parameters
""""""""""

``-c`` / ``--config CONFIG``
    Specify the configuration file to be used. Defaults to ``/etc/barman.conf`` if
    not provided.

``--color`` / ``--colour { never | always | auto }``
    Control whether to use colors in the output. The default is ``auto``. Options are:

    * ``never``: Do not use color.
    * ``always``: Always use color.
    * ``auto``: Use color if the output is to a terminal.

``-d`` / ``--debug``
    Enable debug output. Default is ``false``. Provides detailed logging information for
    troubleshooting.

``-f`` / ``--format { json | console }``
    Specify the output format. Options are:

    * ``json``: Output in JSON format.
    * ``console``: Output in human-readable format (default).

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``--log-level { NOTSET | DEBUG | INFO | WARNING | ERROR | CRITICAL }``
    Override the default logging level. Options are:
    
    * ``NOTSET``: This is the default level when no specific logging level is set. It
      essentially means "no filtering" of log messages, allowing all messages to be
      processed according to the levels that are set in the configuration.
    * ``DEBUG``: This level is used for detailed, diagnostic information, often
      useful for developers when diagnosing problems. It includes messages that are
      more granular and detailed, intended to help trace the execution of the
      program.
    * ``INFO``: This level provides general information about the application's
      normal operation. It's used for messages that indicate the progress of the
      application or highlight key points in the execution flow that are useful but
      not indicative of any issues.
    * ``WARNING``: This level indicates that something unexpected happened or that
      there might be a potential problem. It's used for messages that are not
      critical but could be of concern, signaling that attention might be needed.
    * ``ERROR``: This level is used when an error occurs that prevents a particular
      operation from completing successfully. It's used to indicate significant
      issues that need to be addressed but do not necessarily stop the application
      from running.
    * ``CRITICAL``: This is the highest level of severity, indicating a serious
      error that has likely caused the application to terminate or will have severe
      consequences if not addressed immediately. It's used for critical issues that
      demand urgent attention.

``-q`` / ``--quiet``
    Suppress all output. Useful for cron jobs or automated scripts.

``-v`` / ``--version``
    Show the program version number and exit.
