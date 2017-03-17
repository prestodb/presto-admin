=============================
Advanced Installation Options
=============================

Specifying a Certificate Authority for the Online Installer
-----------------------------------------------------------
The online installer downloads its dependencies from ``pypi.python.org``, the
standard Python location for hosting packages. For some operating systems,
the certificate for pypi.python.org is not included in the CA cert bundle,
so our installation scripts specify ``--trusted-host pypi.python.org`` when
downloading the dependencies.

If using ``--trusted-host`` is not suitable for your security needs, it is
possible to supply your own certificates to use to authenticate to
``pypi.python.org``.  Please note that if these certificates do not work to
access ``pypi.python.org``, the installation will fail. For example, to install
with your own certificates:

::

 ./install-prestoadmin.sh <path_to_cert>/cacert.pem

Coordinator failover
--------------------
Presto does not yet support automatic failover for the coordinator. You can
migrate to a new coordinator using the ``presto-admin`` -H and -x flags
to include and exclude hosts in your command, respectively.

To view these ``presto-admin`` options, use the ``--extended-help`` flag.

You can switch to a new coordinator by following the steps below:

1. Stop Presto on all the nodes where it is running using the command: ::

     ./presto-admin server stop

2. Edit the ``presto-admin`` topology file and replace the old coordinator
   with the new one.  By default, the topology file is located at
   ``~/.prestoadmin/config.json``.

3. To install Presto on the new node, run the following two ``presto-admin``
   commands. The first command is needed only if Java 8 is not already installed
   on the new coordinator: ::

     ./presto-admin package install -H new_coordinator /path/to/jdk8.rpm
     ./presto-admin server install -H new_coordinator /path/to/presto-server.rpm

4. Update the coordinator and worker configuration files controlled by
   ``presto-admin``. By default, these files are available at ``~/.prestoadmin/``.

5. Run the following commands to deploy the new configurations to all nodes,
   including the new coordinator and start the server: ::

     ./presto-admin configuration deploy
     ./presto-admin server start
