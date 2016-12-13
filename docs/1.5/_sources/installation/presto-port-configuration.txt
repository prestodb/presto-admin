.. _presto-port-configuration-label:

===========================
Configuring the Presto Port
===========================

By default, Presto uses 8080 for the HTTP port. If the port is already in use on any given node on your cluster, Presto will not start on that node(s).

To configure the server to use a different port:

1. Select a port that is free on all of the nodes. You can check if a port is already in use on a node by running the following on that node:
::

    netstat -an |grep 8081 |grep LISTEN

It will return nothing if port 8081 is free. 

2. Modify the following properties in ``/etc/opt/prestoadmin/coordinator/config.properties`` and ``/etc/opt/prestoadmin/workers/config.properties``:

::

    http-server.http.port=<port>
    discovery.uri=http://<coordinator_ip_or_host>:<port>


3. Run the following command to deploy the configuration change to the cluster: ::

    sudo ./presto-admin configuration deploy


4. Restart the Presto servers so that the changes get picked up: ::

    sudo ./presto-admin server restart
