===============
Troubleshooting
===============
1. If Presto is not running and you try to execute any command from the Presto CLI you might get:
   ::

    $ Error running command: Server refused connection: http://localhost:8080/v1/statement

   To fix this, start Presto with:
   ::

     $ sudo ./presto-admin server start

2. For troubleshooting problems with presto-admin or Presto, you can use the incident report gathering commands from presto-admin to gather logs and other system information from your cluster. Relevant commands:

 * :ref:`collect-logs`
 * :ref:`collect-query-info`
 * :ref:`collect-system-info`

3. You can find the ``presto-admin`` logs in the ``/var/log/prestoadmin``
   directory.
4. If Presto servers start up successfully but crash shortly thereafter, you
   may have an error in one of your connector configuration files. For example,
   you may have a syntax error or be missing the connector.name property.
   To investigate why a server is not started, you can look at the Presto logs
   in ``/var/log/presto`` on the Presto cluster.  Look at the log with most
   recent timestamp.  You can collect the log information locally using
   :ref:`collect-logs`. To fix an issue with the connectors configuration,
   correct the file and deploy it to the cluster again using
   :ref:`connectors-label`.
5. You can check the status of Presto on your cluster by using
   :ref:`server-status`.
