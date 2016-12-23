===============
Troubleshooting
===============

#. To troubleshoot problems with presto-admin or Presto, you can use the
   incident report gathering commands from presto-admin to gather logs and
   other system information from your cluster. Relevant commands:

    * :ref:`collect-logs`
    * :ref:`collect-query-info`
    * :ref:`collect-system-info`

#. You can find the ``presto-admin`` logs in the ``~/.prestoadmin/log``
   directory.
#. You can check the status of Presto on your cluster by using
   :ref:`server-status`.
#. If Presto is not running and you try to execute any command from the Presto CLI you might get:
   ::

    $ Error running command: Server refused connection: http://localhost:8080/v1/statement

   To fix this, start Presto with:
   ::

     $ ./presto-admin server start

#. If the Presto servers fail to start or crash soon after starting, look at
   the presto server logs on the Presto cluster ``/var/log/presto`` for an
   error message.  You can collect the logs locally using :ref:`collect-logs`.
   The relevant error messages should be at the end of the log with the most
   recent timestamp.  Below are tips for some common errors:

    * Specifying a port that is already in use: Look at
      :ref:`presto-port-configuration-label` to learn how to change the port
      configuration.
    * An error in a catalog configuration file, such as a syntax error or
      a missing connector.name property: correct the file and deploy it to the
      cluster again using :ref:`catalog-add`

#. The following error can occur if you do not have passwordless ssh enabled
   and have not provided a password or if the user requires a sudo password: ::

    Fatal error: Needed to prompt for a connection or sudo password (host: master), but input would be ambiguous in parallel mode

   See :ref:`ssh-configuration-label` for information on setting up
   passwordless ssh and on providing a password, and :ref:`sudo-password-spec`
   for information on providing a sudo password.

#. Support for connecting to a cluster with internal HTTPS and/or LDAP communication
   enabled is experimental. Make sure to check both the Presto server log and the
   ``presto-admin`` log to troubleshoot problems with your configuration; it may also
   be helpful to verify that you can connect to the cluster via the Presto CLI using
   HTTPS/LDAP as appropriate.
