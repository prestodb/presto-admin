
==================
Configuring Presto
==================

Presto configuration parameters can be modified to
tweak performance or add/remove features. While Presto is designed to work well out-of-the-box,
you still may need to make some changes.


Memory configuration
---------------------
It is often necessary to change the default memory configuration based on your cluster's
capacity. The default max memory for each Presto server is 16 GB, but if you have a lot of
memory (say, 120GB/node), you may want to allocate more memory to Presto for better performance.

In order to update the max memory value to 60 GB per node:

1. Change the line in ``/etc/opt/prestoadmin/coordinator/jvm.config`` and
``/etc/opt/prestoadmin/workers/jvm.config`` that says ``-Xmx16G`` to ``-Xmx60G``.

2. Change the following lines in ``/etc/opt/prestoadmin/coordinator/config.properties``
and ``/etc/opt/prestoadmin/workers/config.properties``: ::

    query.max-memory-per-node=8GB
    query.max-memory=50GB


to ::

    query.max-memory-per-node=30GB
    query.max-memory=<30GB * number of nodes>


We recommend setting ``query.max-memory-per-node`` to half of the JVM config max memory, though if your workload is highly concurrent, you may want
to use a lower value for ``query.max-memory-per-node``.

3. Run the following command to deploy the configuration change to the cluster: ::

    sudo ./presto-admin configuration deploy


4. Restart the Presto servers so that the changes get picked up: ::

    sudo ./presto-admin server restart


If you are running Presto in a test environment that has less than 16 GB of memory available,
you will need to follow similar procedures to set the memory configurations lower.

Log file location configurations
---------------------------------

For most production environments, it will be necessary to change the log locations. In order to update these:

1. In ``/etc/opt/prestoadmin/coordinator/node.properties`` and
``/etc/opt/prestoadmin/workers/node.properties`` specify ``node.data-dir``. Presto stores certain logs and other data in
this location. It is very important that this location has enough space for the logs on the filesystem on
each node where Presto is running. The default location for this is ``/var/lib/presto/data``. You can can specify a new
location in ``node.properties``: ::

    node.data-dir=/disk1/presto/data

2. There are two more log related properties that should be specified with locations that have enough space, these
are ``node.launcher-log-file`` and ``node.server-log-file``. You can specify them in ``node.properties``: ::

    node.launcher-log-file=/disk1/presto/launcher.log
    node.server-log-file=/disk2/presto/server.log

They can be in the same directory as ``node.data-dir`` if the filesystem has enough space. For these two log files, the
default location is ``/var/log/presto`` which might not be big enough for a production environment.

3. Run the following command to deploy the configuration change to the cluster: ::

    sudo ./presto-admin configuration deploy

4. Restart the Presto servers so that the changes get picked up: ::

    sudo ./presto-admin server restart

For detailed documentation on ``configuration deploy``, see :ref:`configuration-deploy-label`.
For more configuration parameters, see the Presto documentation.
