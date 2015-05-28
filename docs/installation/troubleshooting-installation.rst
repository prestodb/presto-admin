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

