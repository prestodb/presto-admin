==========================
Presto Server Installation
==========================

Command
-------
::

 presto-admin server install local_path

Description
-----------
This command copies the presto-server rpm from ``local_path`` to all the nodes in the cluster, installs it, deploys the general presto configuration along with tpch connector configuration.
The topology used to configure the nodes are obtained from ``/etc/opt/prestoadmin/config.json``. See :ref:`presto-admin-configuration-label` on how to configure your cluster using config.json. If this file is missing, then the command prompts for user input to get the topology information.

The connectors directory ``/etc/opt/prestoadmin/connectors/`` should contain the configuration files for any catalogs that you would like to connect to in your Presto cluster.
The ``server install`` command will configure the cluster with all the connectors in the directory. If the directory does not exist or is empty prior to ``install``, then by default tpch connector is configured. See <TODO: Link to connectors> on how to add connector configuration files after installation.

Example
-------
::

 sudo ./presto-admin server install /tmp/presto-0.101-1.0.x86_64.rpm

Standalone RPM Install
----------------------

If you want to do a single node installation where coordinator and worker are co-located, you can just use:
::

 rpm -i presto-0.101-1.0.x86_64.rpm

This will deploy the necessary configurations for the presto-server to operate in single-node mode.

========================
RPM Package Installation
========================

Command
-------
::

 presto-admin package install local_path [--nodeps]

Description
-----------
This command copies any rpm from ``local_path`` to all the nodes in the cluster and installs it. Similar to ``server install`` the cluster topology is obtained from the file ``/etc/opt/prestoadmin/config.json``. If this file is missing, then the command prompts for user input to get the topology information.

This command takes an optional ``--nodeps`` flag which indicates if the rpm installed should ignore checking any package dependencies.

.. WARNING:: Using ``--nodeps`` can result in installing the rpm even with any missing dependencies, so you may end up with a broken rpm installation.

Example
-------
::

 sudo ./presto-admin package install /tmp/jdk-8u45-linux-x64.rpm
