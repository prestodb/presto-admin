.. _presto-cli-installation-label:

======================
Running Presto Queries
======================

The Presto CLI provides a terminal-based interactive shell for running queries. The CLI is a self-executing JAR file, which means it acts like a normal UNIX executable.

Download `presto-cli-0.101-executable.jar <https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.101/presto-cli-0.101-executable.jar>`_ and copy it to the location you want to run it from. This location may be any node that has network access to the coordinator. Then rename it to presto and make it executable with chmod +x:
::

 $ mv presto-cli-0.102-executable.jar presto
 $ chmod +x presto

By default, ``presto-admin`` installs the TPC-H connector for you, which generates TPC-H data on-the-fly.  Using this connector, issue the following commands to run your first Presto query:
::

 $ ./presto --catalog tpch --schema tiny
 $ select count(*) from lineitem;

For more on connectors, including how to connect to Hive, see :ref:`connectors-label`.

The above command assumes that you installed the Presto CLI on the coordinator, and that the Presto server is on port 8080. If either of these are not the case, then specify the server location in the command:
::

 $ ./presto --server <host_name>:<port_number> --catalog system

