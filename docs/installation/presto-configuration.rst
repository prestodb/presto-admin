
==================
Configuring Presto
==================

Presto has many configuration parameters that can be modified to
tweak performance or add/remove features. While Presto should work decently well out-of-the-box,
you still may need to make some changes.

For example, it is often necessary to change the default memory configuration
based on your cluster's capacity. The default max memory for each Presto server is 16GB, but in
an environment with less than 16GB available per node, you will have to adjust the settings
to be lower. Similarly, if you have a lot of memory (say, 120GB/node), you may want to allocate more
memory to Presto for better performance.

In the second case, in order to update the max memory value to 60GB per node, 
change the line in ``/etc/opt/prestoadmin/coordinator/jvm.config`` and
``/etc/opt/prestoadmin/workers/jvm.config`` that says ``-Xmx16G`` to ``-Xmx60G``.

In addition, change the following lines in ``/etc/opt/prestoadmin/coordinator/config.properties``
and ``/etc/opt/prestoadmin/workers/config.properties``: ::

    query.max-memory-per-node=8GB
    query.max-memory=50GB


to ::

    query.max-memory-per-node=30GB
    query.max-memory=<30GB * number of nodes>


We recommend setting ``query.max-memory-per-node`` to half of the JVM config max memory, though if your workload is highly concurrent, you may want
to use a lower value for ``query.max-memory-per-node``.

To deploy this configuration change to the cluster, run the following command: ::

    sudo ./presto-admin configuration deploy


Then, restart the Presto servers so that the changes get picked up: ::

    sudo ./presto-admin server restart


For detailed documentation on ``configuration deploy``, see :ref:`configuration-deploy-label`.
For more configuration parameters, see the Presto documentation.
