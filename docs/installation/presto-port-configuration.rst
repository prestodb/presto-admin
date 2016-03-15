.. _presto-port-configuration-label:

=========================
Presto Port Configuration
=========================

By default, Presto uses 8080 for the HTTP port. If the port is already in use on any given node on your cluster, Presto will not start on that node(s).
You can check if that port is already in use on a node by running the following on that node:
::

    netstat -an |grep 8080 |grep LISTEN

It will return nothing if port 8080 is free. 

You can configure the server to use a different port by changing the following properties in ``config.properties`` for both coordinator and workers:

::

    http-server.http.port=<port>
    discovery.uri=http://<coordinator_ip_or_host>:<port>

You can add these properties with the new ``port`` to ``/etc/opt/prestoadmin/coordinator/config.properties`` and
``/etc/opt/prestoadmin/workers/config.properties``. Then, run :ref:`configuration-deploy-label`.
