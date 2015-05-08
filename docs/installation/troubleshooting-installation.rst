===============
Troubleshooting
===============
1. If the presto servers are not running and you try to execute any command from presto-cli you might get
 ::

 $ Error running command: Server refused connection: http://localhost:8080/v1/statement

 To fix this, start the presto servers by
 ::

 $ sudo ./presto-admin server start