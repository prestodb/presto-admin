=====================
Presto-Admin Commands
=====================

.. _collect-logs:

************
collect logs
************
::

    presto-admin collect logs

This command gathers Presto server logs and launcher logs from the ``/var/log/presto/`` directory across the cluster along with the
``/var/log/prestoadmin/presto-admin.log`` and creates a tar file. The final tar output will be saved at ``/tmp/presto-debug-logs.tar.bz2``.


Example
-------
::

    sudo ./presto-admin collect logs

.. _collect-query-info:

******************
collect query_info
******************
::

    presto-admin collect query_info <query_id>

This command gathers information about a Presto query identified by the given ``query_id`` and stores that information in a JSON file.
The output file will be saved at ``/tmp/presto-debug/query_info_<query_id>.json``.

Example
-------
::

    sudo ./presto-admin collect query_info 20150525_234711_00000_7qwaz

.. _collect-system-info:

*******************
collect system_info
*******************
::

    presto-admin collect system_info

This command gathers various system specific information from the cluster. The information is saved in a tar file at ``/tmp/presto-debug-sysinfo.tar.bz2``.
The gathered information includes:

 * Node specific information from Presto like node uri, last response time, recent failures, recent requests made to the node, etc.
 * Connectors configured
 * Other system specific information like OS information, Java version, ``presto-admin`` version and Presto server version

Example
-------
::

    sudo ./presto-admin collect system_info


.. _configuration-deploy-label:

********************
configuration deploy
********************
::

    presto-admin configuration deploy [coordinator|workers]

This command deploys `Presto configuration files <https://prestodb.io/docs/current/installation/deployment.html>`_
onto the cluster. ``presto-admin`` uses different configuration directories for
worker and coordinator configurations so that you can easily create different
configurations for your coordinator and worker nodes. Create a
``/etc/opt/prestoadmin/coordinator`` directory for your coordinator
configurations and a ``/etc/opt/prestoadmin/workers`` directory for your
workers configuration.  Place the configuration files for the coordinator
and workers in their respective directories. The optional ``coordinator`` or ``workers``
argument tells ``presto-admin`` to only deploy the coordinator or workers
configurations. To deploy both configurations at once, don't specify either
option.

When you run configuration deploy, the following files will be deployed to
the ``/etc/presto`` directory on your Presto cluster:

* node.properties
* config.properties
* jvm.config
* log.properties (if it exists)

.. NOTE:: This command will not deploy the configurations for connectors.  To deploy connector configurations run `connector add`_

If the coordinator is also a worker, it will get the coordinator configuration.
The deployed configuration files will overwrite the existing configurations on
the cluster. However, the node.id from the
node.properties file will be preserved. If no node.id exists, a new id will be
generated. If any required files are absent when you run configuration deploy,
a default configuration will be deployed. Below are the default
configurations:

*node.properties* ::

    node.environment=presto
    node.data-dir=/var/lib/presto/data
    plugin.config-dir=/etc/presto/catalog
    plugin.dir=/usr/lib/presto/lib/plugin

.. NOTE:: Do not change the value of plugin.config-dir=/etc/presto/catalog as it is necessary for presto to be able to find the catalog directory when Presto has been installed by RPM.

*jvm.config* ::

    -server
    -Xmx16G
    -XX:-UseBiasedLocking
    -XX:+UseG1GC
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:+UseGCOverheadLimit
    -XX:OnOutOfMemoryError=kill -9 %p
    -DHADOOP_USER_NAME=hive

*config.properties*

For workers: ::

    coordinator=false
    discovery.uri=http://<coordinator>:8080
    http-server.http.port=8080
    query.max-memory-per-node=8GB
    query.max-memory=50GB

For coordinator: ::

    coordinator=true
    discovery-server.enabled=true
    discovery.uri=http://<coordinator>:8080
    http-server.http.port=8080
    node-scheduler.include-coordinator=false
    query.max-memory-per-node=8GB
    query.max-memory=50GB

    # if the coordinator is also a worker, it will have the following property instead
    node-scheduler.include-coordinator=true

See :ref:`presto-port-configuration-label` for details on http port configuration.

Example
-------
If you want to change the jvm configuration on the coordinator and the
``node.environment`` property from ``node.properties`` on all nodes, add the
following ``jvm.config`` to ``/etc/opt/prestoadmin/coordinator``

.. code-block:: none

    -server
    -Xmx16G
    -XX:-UseBiasedLocking
    -XX:+UseG1GC
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:+UseGCOverheadLimit
    -XX:OnOutOfMemoryError=kill -9 %p

Further, add the following ``node.properties`` to
``/etc/opt/prestoadmin/coordinator`` and ``/etc/opt/prestoadmin/workers``: ::

    node.environment=test
    node.data-dir=/var/lib/presto/data
    plugin.config-dir=/etc/presto/catalog
    plugin.dir=/usr/lib/presto/lib/plugin

Then run: ::

    sudo ./presto-admin configuration deploy

This will distribute to the coordinator a default ``config.properties``, the new
``jvm.config`` and ``node.properties``.  The workers will
receive the default ``config.properties`` and ``jvm.config``, and the same
``node.properties`` as the coordinator.

If instead you just want to update the coordinator configuration, run: ::

    sudo ./presto-admin configuration deploy coordinator

This will leave the workers configuration as it was, but update the
coordinator's configuration

******************
configuration show
******************
::

    presto-admin configuration show [node|jvm|config|log]

This command prints the contents of the Presto configuration files deployed in the cluster. It takes an optional configuration name argument for the configuration files node.properties, jvm.config, config.properties and log.properties. For missing configuration files a warning will be printed except for log.properties file, since it is an optional configuration file in your Presto cluster.

If no argument is specified, then all four configurations will be printed.

Example
-------
::

    sudo ./presto-admin configuration show node

.. _connector-add:

*************
connector add
*************
::

    presto-admin connector add [<name>]

This command is used to deploy connector configurations to the Presto cluster.
`Connector configurations <https://prestodb.io/docs/current/connector.html>`_ are
kept in the configuration directory ``/etc/opt/prestoadmin/connectors``

To add a connector using ``presto-admin``, first create a configuration file in
``/etc/opt/prestoadmin/connectors``. The file should be named
``<name>.properties`` and contain the configuration for that connector.

Use the optional ``name`` argument to add a particular connector to your
cluster. To deploy all connectors in the connectors configuration directory,
leave the name argument out.

In order to query using the newly added connector, you need to restart the
Presto server (see `server restart`_): ::

    presto-admin server restart

Example
-------
To add the jmx connector, create a file
``/etc/opt/prestoadmin/connectors/jmx.properties`` with the content
``connector.name=jmx``.
Then run: ::

    sudo ./presto-admin connector add jmx
    sudo ./presto-admin server restart

If you have two connectors in the configuration directory, for example
``jmx.properties`` and ``dummy.properties``, and would like to deploy both at
once, you could run ::

    sudo ./presto-admin connector add
    sudo ./presto-admin server restart

Adding a Custom Connector
-------------------------
In order to install a custom connector not included with Presto, the jar must be
added to the Presto plugin location using the ``plugin add_jar`` command before
running the ``connector add`` command.

Example: ::

   sudo ./presto-admin plugin add_jar my_connector.jar my_connector
   sudo ./presto-admin connector add my_connector
   sudo ./presto-admin server restart

The ``add_jar`` command assumes the default plugin location of
``/usr/lib/presto/lib/plugin`` (see `plugin add_jar`_).  As with the default
connectors, a ``my_connector.properties`` file must be created. Refer to the
custom connector's documentation for the properties to specify.

The ``plugin add_jar`` command works with both jars and directories containing jars.

****************
connector remove
****************
::

    presto-admin connector remove <name>

The connector remove command is used to remove a connector from your presto
cluster configuration. Running the command will remove the connector from all
nodes in the Presto cluster. Additionally, it will remove the local
configuration file for the connector.

In order for the change to take effect, you will need to restart services. ::

    presto-admin server restart


Example
-------
For example: To remove the jmx connector, run ::

    sudo ./presto-admin connector remove jmx
    sudo ./presto-admin server restart


***************
package install
***************

::

    presto-admin package install local_path [--nodeps]

This command copies any rpm from ``local_path`` to all the nodes in the cluster and installs it. Similar to ``server install`` the cluster topology is obtained from the file ``/etc/opt/prestoadmin/config.json``. If this file is missing, then the command prompts for user input to get the topology information.

This command takes an optional ``--nodeps`` flag which indicates if the rpm installed should ignore checking any package dependencies.

.. WARNING:: Using ``--nodeps`` can result in installing the rpm even with any missing dependencies, so you may end up with a broken rpm installation.

Example
-------
::

    sudo ./presto-admin package install /tmp/jdk-8u45-linux-x64.rpm


**************
plugin add_jar
**************
::

    presto-admin plugin add_jar <local-path> <plugin-name> [<plugin-dir>]

This command deploys the jar at ``local-path`` to the plugin directory for
``plugin-name``.  By default ``/usr/lib/presto/lib/plugin`` is used as the
top-level plugin directory. To deploy the jar to a different location, use the
optional ``plugin-dir`` argument.

Example
-------
::

    sudo ./presto-admin plugin add_jar connector.jar my_connector
    sudo ./presto-admin plugin add_jar connector.jar my_connector /my/plugin/dir

The first example will deploy connector.jar to
``/usr/lib/presto/lib/plugin/my_connector/connector.jar``
The second example will deploy it to ``/my/plugin/dir/my_connector/program.jar``.

**********
script run
**********
::

    presto-admin script run <local-path-to-script> [<remote-dir-to-put-script>]

This command can be used to run an arbitrary script on a cluster. It copies the
script from its local location to the specified remote directory (defaults to
/tmp), makes the file executable, and runs it.

Example
-------
::

    sudo ./presto-admin script run /my/local/script.sh
    sudo ./presto-admin script run /my/local/script.sh /remote/dir


.. _server-install-label:

**************
server install
**************
::

    presto-admin server install <local_path>

This command copies the presto-server rpm from ``local_path`` to all the nodes in the cluster, installs it, deploys the general presto configuration along with tpch connector configuration. The ``local_path`` should be accessible by ``presto-admin``.
The topology used to configure the nodes are obtained from ``/etc/opt/prestoadmin/config.json``. See :ref:`presto-admin-configuration-label` on how to configure your cluster using config.json. If this file is missing, then the command prompts for user input to get the topology information.

The general configurations for Presto's coordinator and workers are taken from the directories ``/etc/opt/prestoadmin/coordinator`` and ``/etc/opt/prestoadmin/workers`` respectively. If these directories or any required configuration files are absent when you run ``server install``, a default configuration will be deployed. See `configuration deploy`_ for details.

The connectors directory ``/etc/opt/prestoadmin/connectors/`` should contain the configuration files for any catalogs that you would like to connect to in your Presto cluster.
The ``server install`` command will configure the cluster with all the connectors in the directory. If the directory does not exist or is empty prior to ``server install``, then by default the tpch connector is configured. See `connector add`_ on how to add connector configuration files after installation.

Example
-------
::

    sudo ./presto-admin server install /tmp/presto.rpm

**Standalone RPM Install**

If you want to do a single node installation where coordinator and worker are co-located, you can just use:
::

    rpm -i presto.rpm

This will deploy the necessary configurations for the presto-server to operate in single-node mode.

.. _server-restart-label:

**************
server restart
**************
::

    presto-admin server restart

This command first stops any Presto servers running and then starts them. A status check is performed on the entire cluster and is reported at the end.

Example
-------
::

    sudo ./presto-admin server restart


.. _server-start-label:

************
server start
************
::

    presto-admin server start

This command starts the Presto servers on the cluster. A status check is performed on the entire cluster and is reported at the end.

Example
-------
::

    sudo ./presto-admin server start


.. _server-status:

*************
server status
*************
::

    presto-admin server status

This command prints the status information of Presto in the cluster. This command will
fail to report the correct status if the Presto installed is older than version 0.100. It will not print any status information if a given node is inaccessible.

The status output will have the following information:
    * server status
    * node uri
    * Presto version installed
    * node is active/inactive
    * connectors deployed

Example
-------
::

    sudo ./presto-admin server status


***********
server stop
***********
::

    presto-admin server stop

This command stops the Presto servers on the cluster.

Example
-------
::

    sudo ./presto-admin server stop


****************
server uninstall
****************
::

    presto-admin server uninstall

This command stops the Presto server if running on the cluster and uninstalls the Presto rpm. The uninstall command removes any presto
related files deployed during ``server install`` but retains the Presto logs at ``/var/log/presto``.

Example
-------
::

    sudo ./presto-admin server uninstall


**************
server upgrade
**************
::

    presto-admin server upgrade local_package_path [local_config_dir]

This command upgrades the Presto RPM on all of the nodes in the cluster to the RPM specified
by ``local_package_path``, preserving the existing configuration on the cluster. The existing
cluster configuration is saved locally to local_config_dir (which defaults to a temporary
folder if not specified).

This command can also be used to downgrade the Presto installation, if the RPM at ``local_package_path``
is an earlier version than the Presto installed on the cluster.

Note that if the configuration files on the cluster differ from the presto-admin configuration
files found in ``/etc/opt/prestoadmin``, the presto-admin configuration files are not updated.

Example
-------
::

    sudo ./presto-admin server upgrade new-rpm.rpm /tmp/cluster-configuration


*************
topology show
*************
::

 presto-admin topology show

This command shows the current topology configuration for the cluster (including the coordinators, workers, SSH port, and SSH username).

Example
-------
::

    sudo ./presto-admin topology show


