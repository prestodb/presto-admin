===============
Troubleshooting
===============

#. To troubleshoot problems with presto-admin or Presto, you can use the
   incident report gathering commands from presto-admin to gather logs and
   other system information from your cluster. Relevant commands:

    * :ref:`collect-logs`
    * :ref:`collect-query-info`
    * :ref:`collect-system-info`

#. You can find the ``presto-admin`` logs in the ``/var/log/prestoadmin``
   directory.
#. You can check the status of Presto on your cluster by using
   :ref:`server-status`.
#. If Presto is not running and you try to execute any command from the Presto CLI you might get:
   ::

    $ Error running command: Server refused connection: http://localhost:8080/v1/statement

   To fix this, start Presto with:
   ::

     $ sudo ./presto-admin server start

#. If Presto servers start up successfully but crash shortly thereafter, you
   may have an error in one of your connector configuration files. For example,
   you may have a syntax error or be missing the connector.name property.
   To investigate why a server is not started, you can look at the Presto logs
   in ``/var/log/presto`` on the Presto cluster.  Look at the log with most
   recent timestamp.  You can collect the log information locally using
   :ref:`collect-logs`. To fix an issue with the connectors configuration,
   correct the file and deploy it to the cluster again using
   :ref:`connectors-label`.
#. The following error can occur if you do not have passwordless ssh enabled
   and have not provided a password or if the user requires a sudo password: ::

    Fatal error: Needed to prompt for a connection or sudo password (host: master), but input would be ambiguous in parallel mode

   See :ref:`ssh-configuration-label` for information on setting up
   passwordless ssh and on providing a password, and :ref:`sudo-password-spec`
   for information on providing a sudo password.
