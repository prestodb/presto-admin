.. _presto-port-configuration-label:

=========================
Presto Port Configuration
=========================

By default, Presto configuration uses 8080 for the HTTP port. If the port is already used in your cluster, the server will fail to start.
You can check if the port is already in use by running (it will return nothing if the port 8080 is free):
::

    netstat -an |grep 8080 |grep LISTEN

However, you can configure it to use a different port by changing the following properties in ``config.properties`` for both coordinator and workers:

::

    http-server.http.port=<port>
    discovery.uri=http://<coordinator>:<port>

You can add these properties with the new ``port`` to the file ``config.properties`` under the directories ``/etc/opt/prestoadmin/coordinator``
and ``/etc/opt/prestoadmin/workers`` locally, prior to running :ref:`server-install-label`. If you have already installed Presto using :ref:`server-install-label` then you
can reconfigure the cluster to use a new port by updating the ``config.properties`` at  ``/etc/opt/prestoadmin/coordinator``
and ``/etc/opt/prestoadmin/workers`` and then running :ref:`configuration-deploy-label`.