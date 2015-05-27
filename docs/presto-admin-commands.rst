=====================
Presto Admin Commands
=====================

**************
server install
**************

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

***************
package install
***************

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

*************
topology show
*************

 presto-admin topology show

Description
-----------
This command shows the current topology configuration for the cluster (including the coordinators, workers, SSH port, and SSH username).

Example
-------
::

 sudo ./presto-admin topology show

******************
configuration show
******************
 presto-admin configuration show [node|jvm|config|log]

Description
-----------

This command prints the contents of the Presto configuration files deployed in the cluster. It takes an optional configuration name argument for the configuration files node.properties, jvm.config, config.properties and log.properties. For missing configuration files a warning will be printed except for log.properties file, since it is an optional configuration file in your Presto cluster.

If no argument is specified, then all four configurations will be printed.

Example
-------
::

 sudo ./presto-admin configuration show node

.. _connectors-label:

*************
Connector Add
*************

Command
-------
::
presto-admin connector add [<name>]

Description
-----------
This command is used to deploy connector configurations to the Presto cluster.
[TODO: link to Presto connector configuration.]  Connector configurations are
kept in the configuration directory ``/etc/opt/prestoadmin/connectors``

To add a connector using presto-admin, first create a configuration file in
``/etc/opt/prestoadmin/connectors``. The file should be named
``<name>.properties`` and contain the configuration for that connector.

Use the optional ``name`` argument to add a particular connector to your
cluster. To deploy all connectors in the connectors configuration directory,
leave the name argument out.

In order to query using the newly added connector, you need to restart [TODO: link to server restart] the
Presto server: ::

    presto-admin server restart

Example
-------
To add the jmx connector, create a file
``/etc/opt/prestoadmin/connectors/jmx.properties`` with the content
``connector.name=jmx``.
Then run: ::

    presto-admin connector add jmx
    presto-admin server restart

If you have two connectors in the configuration directory, for example
``jmx.properties`` and ``dummy.properties``, and would like to deploy both at
once, you could run ::

    presto-admin connector add
    presto-admin server restart

****************
Connector Remove
****************

Command
-------
::

    presto-admin connector remove <name>

Description
-----------
The connector remove command is used to remove a connector from your presto
cluster configuration. Running the command will remove the connector from all
nodes in the Presto cluster. Additionally, it will remove the local
configuration file for the connector.

To remove a connector, run: ::

    presto-admin connector remove <connector>

In order for the change to take effect, you will need to restart services. ::

    presto-admin server restart


Example
-------
For example: To remove the jmx connector, run ::

    presto-admin connector remove jmx
    presto-admin server restart
