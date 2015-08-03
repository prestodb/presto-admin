.. _presto-port-configuration-label:

=========================
Presto Port Configuration
=========================

By default, Presto configuration uses 8080 for the HTTP port. If the port is already used on a given node on your cluster, that server will fail to start.
You can check if that port is already in use on a node by running the following on that node:
::

    netstat -an |grep 8080 |grep LISTEN

It will return nothing if port 8080 is free. 

You can configure the server to use a different port by changing the following properties in ``config.properties`` for both coordinator and workers:

::

    http-server.http.port=<port>
    discovery.uri=http://<coordinator_ip_or_host>:<port>

You can add these properties with the new ``port`` to the file ``config.properties`` under each of the directories ``/etc/opt/prestoadmin/coordinator``
and ``/etc/opt/prestoadmin/workers`` locally, prior to running :ref:`server-install-label`. You may need to create these two directories. 
If you have already installed Presto using :ref:`server-install-label` then you
can reconfigure the cluster to use a new port by updating the ``config.properties`` at  ``/etc/opt/prestoadmin/coordinator``
and ``/etc/opt/prestoadmin/workers`` and then running :ref:`configuration-deploy-label`.

``presto-admin`` :ref:`server-start-label` and :ref:`server-restart-label` will check if the port configured for the server
is already in use. If the port is in use on a node, then ``presto-admin`` will issue a warning and skip starting the server on that particular node.
