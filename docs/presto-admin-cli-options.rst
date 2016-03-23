=================================
Presto-Admin Command-Line Options
=================================

A quick overview of the possible CLI options for ``presto-admin`` can be found
via ``sudo ./presto-admin --extended-help``. More details on those options can
be found below.

--version
    Prints out the current ``presto-admin`` version and exits.

-h, --help
    Prints out a usage string, the basic ``presto-admin`` options and the
    available commands, then exits.

-d, --display
    Prints detailed information about a given command.

    e.g., to get detailed information about the ``server install`` command, enter: ::

        sudo ./presto-admin -d server install

--extended-help
    Prints out a usage string, all the ``presto-admin`` options and the
    available commands, then exits.

-I, --initial-password-prompt
    Forces password prompt before running any commands on the cluster.

    Either this option or the ``--password`` option is necessary if the user from
    ``/etc/opt/prestoadmin/config.json`` needs a password for sudo.

    Note that the SSH password and the sudo password must be the same,
    if passwordless SSH is not used.

-p PASSWORD, --password=PASSWORD
    Sets password for use with authentication and/or sudo.

    Either this option or the ``--initial-password-prompt`` option is necessary
    if the user from ``/etc/opt/prestoadmin/config.json`` needs a password for sudo.

    Note that the SSH password and the sudo password must be the same,
    if passwordless SSH is not used.

--abort-on-error
    Aborts the command, instead of warning, if a command fails on any node. The
    default for ``presto-admin`` is to warn if a command fails on any node.

-a, --no_agent
    Forces ``presto-admin`` not to seek out running SSH agents when using
    key-based authentication.

-A, --forward-agent
    Enables forwarding of a local SSH agent to the remote end.

--colorize-errors
    Colorizes error output.

-D, --disable-known-hosts
    Turns off loading of a user's SSH known_hosts file. Disabling known_hosts leaves
    you vulnerable to man-in-the-middle attacks. However,in some environments like
    EC2, a particular host getting a different key should not mean that you are not
    able to connect via SSH to that host.

-g HOST, --gateway=HOST
    Routes SSH connections through the SSH daemon on the
    specified gateway host to their final destination.

-H HOSTS, --hosts=HOSTS
    Sets the list of hosts where a ``presto-admin`` command should be executed.
    The values should be comma-separated and exist in your topology.

-i PATH
    Adds the SSH private key file specified by PATH to the set of keys to
    try during key-based SSH authentication. May be repeated.

-k, --no-keys
    Disables loading private key files from ``~/.ssh/``.

--keepalive=N
    Sends an SSH keepalive every N seconds to keep SSH from timing out.

-n M, --connection-attempts=M
    Makes M attempts to connect before giving up. The default number of attempts to try is 1.

--port=PORT
    Sets the SSH connection port. If the SSH port is set both in
    ``/etc/opt/prestoadmin/config.json`` and on the command line, the port
    specified on the command line will be used.

-r, --reject-unknown-hosts
    Aborts when a host is not in the user's SSH ``known_hosts`` file.

--system-known-hosts=SYSTEM_KNOWN_HOSTS
    Loads the given SSH ``known_hosts`` file before reading the user's ``known_hosts``
    file.

-t N, --timeout=N
    Sets the network connection timeout to N seconds. The default is 10 seconds.

-T N, --command-timeout=N
    Sets the timeout for the given remote command to N seconds. The default is
    to have no timeout.

-u USER, --user=USER
    Sets the user that is used for SSH connections. If the SSH username is set both in
    ``/etc/opt/prestoadmin/config.json`` and on the command line, the username
    specified on the command line will be used.

-x HOSTS, --exclude-hosts=HOSTS
    Sets the list of hosts to be excluded when executing a ``presto-admin``
    command. The values should be comma-separated and exist in your topology.

--serial
    Switches to run the command in serial. The default is to run in parallel, because
    parallel mode is usually faster. However, if you want a password prompt while the command
    is running (without specifying ``-I`` or ``--initial-password-prompt``), the ``--serial`` flag is necessary.
