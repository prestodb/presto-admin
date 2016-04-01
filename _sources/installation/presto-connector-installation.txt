
==================
Adding a Connector
==================

In Presto, connectors allow you to access different data sources -- e.g.,
Hive, PostgreSQL, or MySQL.

To add the Hive connector:
 
1. Create a file ``hive.properties`` in ``/etc/opt/prestoadmin/connectors`` with the following content: ::

    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://<metastore-host-or-ip>:<metastore-port>


2. Distribute the configuration file to all of the nodes in the cluster: ::

    sudo ./presto-admin connector add hive


3. Restart Presto: ::

    sudo ./presto-admin server restart


You may need to add additional properties for the Hive connector to work properly, such as if your Hadoop cluster
is set up for high availability. For these and other properties, see the `Hive connector documentation <https://prestodb.io/docs/current/connector/hive.html>`_.

For detailed documentation on ``connector add``, see :ref:`connector-add`.
For more on which connectors Presto supports, see the `Presto connector documentation <https://prestodb.io/docs/current/connector.html>`_.
