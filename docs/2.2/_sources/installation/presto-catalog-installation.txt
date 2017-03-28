
================
Adding a Catalog
================

In Presto, connectors allow you to access different data sources -- e.g.,
Hive, PostgreSQL, or MySQL.

To add a catalog for the Hive connector:

1. Create a file ``hive.properties`` in ``~/.prestoadmin/catalog`` with the following content: ::

    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://<metastore-host-or-ip>:<metastore-port>


2. Distribute the configuration file to all of the nodes in the cluster: ::

    ./presto-admin catalog add hive


3. Restart Presto: ::

    ./presto-admin server restart


You may need to add additional properties for the Hive connector to work properly, such as if your Hadoop cluster
is set up for high availability. For these and other properties, see the `Hive connector documentation <https://prestodb.io/docs/current/connector/hive.html>`_.

For detailed documentation on ``catalog add``, see :ref:`catalog-add`.
For more on which catalogs Presto supports, see the `Presto connector documentation <https://prestodb.io/docs/current/connector.html>`_.
