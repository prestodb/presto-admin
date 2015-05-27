===============
Troubleshooting
===============
1. If Presto is not running and you try to execute any command from the Presto CLI you might get:
::

 $ Error running command: Server refused connection: http://localhost:8080/v1/statement

To fix this, start Presto with:
::

 $ sudo ./presto-admin server start

