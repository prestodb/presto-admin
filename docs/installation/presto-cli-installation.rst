.. _presto-cli-installation-label:

=======================
Presto CLI Installation
=======================

The Presto CLI provides a terminal-based interactive shell for running queries. The CLI is a self-executing JAR file, which means it acts like a normal UNIX executable.

Download `presto-cli-0.102-executable.jar <https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.102/presto-cli-0.102-executable.jar>`_ and copy it to the location you want to run it from. This location may be any node that has network access to the coordinator. Then rename it to presto and make it executable with chmod +x.
::

 $ mv presto-cli-0.102-executable.jar presto
 $ chmod +x presto

Now connect to issue your first Presto query!
::

 $ ./presto --catalog tpch --schema tiny
 $ select count(*) from lineitem;


This assumes the default server location of localhost:8080. If you have the Presto CLI on a different node from the coordinator then specify it in the command.
::

 $ ./presto --server <host_name>:<port_number> --catalog system
