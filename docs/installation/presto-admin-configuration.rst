.. _presto-admin-configuration-label:

==========================
Presto Admin Configuration
==========================
A Presto cluster consists of a coordinator node and one or more workers nodes. A coordinator and worker may be located on the same node, meaning that you can have a single-node installation of Presto, but having a dedicated node for the coordinator is recommended for better performance, especially on larger clusters.

In order to use ``presto-admin`` to manage software on a cluster of nodes, you must specify a configuration for ``presto-admin``. This configuration indicates the nodes on which to install as well as other credentials.

To setup a configuration, create a file ``/etc/opt/prestoadmin/config.json`` with the content below. Replace the variables denoted with brackets <> with actual values enclosed in quotations. The user specified by ``username`` must have sudo access on all of Presto nodes, and ``presto-admin`` also must be able to login to all of the nodes via SSH as that user (see :ref:`ssh-configuration-label` for details on how to set that up). The file should be owned by root with R/W permissions (i.e. 622).
::

 {
 "username": "<ssh_user_name>",
 "port": "<ssh_port>",
 "coordinator": "<host_name>",
 "workers": ["<host_name_1>", "<host_name_2>", ... "<host_name_n>"]
 }

All of the properties are optional. If you include a blank ``config.json`` file, the following default configuration is used:
::

 {
 "username": "root",
 "port": "22",
 "coordinator": "localhost",
 "workers": ["localhost"]
 }

The above configuration is for a single-node installation on the same node as ``presto-admin`` is installed on. You can also specify some but not all of the properties. For example, for a 6 node cluster with default username and port, a sample ``config.json`` would be:

::

 {
 "coordinator": "master",
 "workers": ["slave1","slave2","slave3","slave4","slave5"]
 }

