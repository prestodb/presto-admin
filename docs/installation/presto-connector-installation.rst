
==================
Adding a Connector
==================

In Presto, connectors allow you to access different data sources -- e.g.,
Hive, PostgreSQL, or MySQL.

To add the Hive connector, create a file ``hive.properties`` in ``/etc/opt/prestoadmin/connectors``.

For Cloudera CDH 5, it should have the following content: ::

    connector.name=hive-cdh5
    hive.metastore.uri=thrift://<metastore-host-or-ip>:<metastore-port>


For Hadoop 2.0+ (including HDP), it should have the following content: ::

   connector.name=hive-hadoop2
   hive.metastore.uri=thrift://<metastore-host-or-ip>:<metastore-port>


There are additional properties and possible configurations in the
`Hive connector documentation <https://prestodb.io/docs/current/connector/hive.html>`_,
for example if you have a HA Hadoop cluster.

After adding a connector configuration file, :ref:distribute <reference to connector add> it to all of the nodes in the cluster: ::

    sudo ./presto-admin connector add hive

Once the new connector configuration has been distributed, :ref:restart <reference to server restart> Presto: ::

    sudo ./presto-admin server restart


For more on what connectors Presto supports, see the `Presto connector documentation <https://prestodb.io/docs/current/connector.html>`_.
