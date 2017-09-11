.. _presto-configuration-label:

==================
Configuring Presto
==================

Presto configuration parameters can be modified to
tweak performance or add/remove features. While Presto is designed to work well out-of-the-box,
you still may need to make some changes.


Memory configuration
--------------------
It is often necessary to change the default memory configuration based on your cluster's
capacity. The default max memory for each Presto server is 16 GB, but if you have a lot of
memory (say, 120GB/node), you may want to allocate more memory to Presto for better performance.

In order to update the max memory value to 60 GB per node:

1. Change the line in ``~/.prestoadmin/coordinator/jvm.config`` and
   ``~/.prestoadmin/workers/jvm.config`` that says ``-Xmx16G`` to ``-Xmx60G``.

2. Change the following lines in ``~/.prestoadmin/coordinator/config.properties``
   and ``~/.prestoadmin/workers/config.properties``: ::

        query.max-memory-per-node=8GB
        query.max-memory=50GB


   to ::

        query.max-memory-per-node=30GB
        query.max-memory=<30GB * number of nodes>


   We recommend setting ``query.max-memory-per-node`` to half of the JVM config max memory, though if your workload is highly concurrent, you may want
   to use a lower value for ``query.max-memory-per-node``. If you have large data skew, ``query.max-memory-per-node`` should.
   By default in Presto 148t and higher, ``query.max-memory-per-node`` is 10% of the ``Xmx`` value specified in ``jvm.config``.

3. Run the following command to deploy the configuration change to the cluster: ::

        ./presto-admin configuration deploy


4. Restart the Presto servers so that the changes get picked up: ::

        ./presto-admin server restart


   If you are running Presto in a test environment that has less than 16 GB of memory available,
   you will need to follow similar procedures to set the memory configurations lower.

Log file location configurations
--------------------------------

For most production environments, it will be necessary to change the log locations. In order to update these:

1. Stop the Presto server. ::

    ./presto-admin server stop

2. Presto stores logs and other data in ``node.data-dir``, ``node.launcher-log-file``,
   and ``node.server-log-file``. It is very important that these locations have enough space for the logs on the filesystem on
   each node where Presto is running. The default location for ``node.data-dir`` is ``/var/lib/presto/data``, the
   default location for ``node.launcher-log-file`` is ``/var/log/presto/launcher.log``, and the default
   location for ``node.server-log-file`` is ``/var/log/presto/server.log``.
   Assuming the chosen locations are ``/data1/presto`` and ``/data2/presto`` for the data directory
   and server logs respectively, the properties in ``~/.prestoadmin/coordinator/node.properties`` and
   ``~/.prestoadmin/workers/node.properties`` will be as follows: ::

        node.data-dir=/data1/presto/data
        node.launcher-log-file=/data2/presto/launcher.log
        node.server-log-file=/data2/presto/server.log

3. The log directory(ies) (in the above example, ``/data1/presto`` and ``/data2/presto``; the ``data`` directory
   for ``node.data-dir`` is created by Presto) need to
   exist on all nodes and be owned by the ``presto`` user. The command ``presto-admin run_script``
   can be used to perform these actions on all of the nodes. First, create a script in the same
   directory as ``presto-admin``, called ``script.sh``: ::

        #!/bin/bash
        mkdir -p /data1/presto
        mkdir -p /data2/presto
        chown presto:presto /data1/presto
        chown presto:presto /data2/presto

   Then, run the following command: ::

        ./presto-admin run_script script.sh

4. Run the following command to deploy the log configuration change to the cluster: ::

    ./presto-admin configuration deploy

5. Restart the Presto servers so that the changes get picked up: ::

    ./presto-admin server restart


For detailed documentation on ``configuration deploy``, see :ref:`configuration-deploy-label`.
For more configuration parameters, see the Presto documentation.
