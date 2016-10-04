.. _advanced-installation-options-label:

=============================
Advanced Installation Options
=============================

Online vs Offline Installer
---------------------------
The installer has two different versions: the offline installer -- 
``prestoadmin-1.5-offline.tar.bz2``-- and the online installer --
``prestoadmin-1.5-online.tar.bz2``. The offline installer includes all of the
dependencies for ``presto-admin``, so it can be used on a cluster without an 
outside network connection. The offline installer is recommended because it is faster.

The online installer downloads all of the dependencies when you call ``./install-prestoadmin.sh``. 
You may need to use the online installer if you try to install ``presto-admin`` on an unsupported 
operating system, because some of the binary files are dependent on the 
system files of a given operating system. Be aware, though, that there may be other 
operating system dependent differences beyond the installation process for 
unsupported operating system, and ``presto-admin`` may not work.

For instructions on how to build either installer see the
`README <https://github.com/prestodb/presto-admin>`_ in the presto-admin
repository.

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

 sudo ./install-prestoadmin.sh <path_to_cert>/cacert.pem

Upgrading Presto-Admin
----------------------
Upgrading to a newer version of ``presto-admin`` requires deleting the old
installation and then installing the new version.  The recommended installation
location is ``/opt/prestoadmin``, but you may have installed it in an
alternative location.  After you've deleted the ``prestoadmin`` directory,
install the newer version of ``presto-admin`` by following the instructions in
the installation section (see :ref:`presto-admin-installation-label`). Your
configuration files located in ``/etc/opt/prestoadmin`` will remain intact and
continue to be used by the newer version of ``presto-admin``.

Coordinator failover
--------------------
Presto does not yet support automatic failover for the coordinator. You can
migrate to a new coordinator using the ``presto-admin`` -H and -x flags
to include and exclude hosts in your command, respectively.

To view these ``presto-admin`` options, use the ``--extended-help`` flag.

You can switch to a new coordinator by following the steps below:

1. Stop Presto on all the nodes where it is running using the command: ::

     sudo ./presto-admin server stop

2. Edit the ``presto-admin`` topology file and replace the old coordinator
   with the new one.  By default, the topology file is located at
   ``/etc/opt/prestoadmin/config.json``.

3. To install Presto on the new node, run the following two ``presto-admin``
   commands. The first command is needed only if Java 8 is not already installed
   on the new coordinator: ::

     sudo ./presto-admin package install -H new_coordinator /path/to/jdk8.rpm
     sudo ./presto-admin server install -H new_coordinator /path/to/presto-server.rpm

4. Update the coordinator and worker configuration files controlled by
   ``presto-admin``. By default, these files are available at ``/etc/opt/prestoadmin/``.

5. Run the following commands to deploy the new configurations to all nodes,
   including the new coordinator and start the server: ::

     sudo ./presto-admin configuration deploy
     sudo ./presto-admin server start
