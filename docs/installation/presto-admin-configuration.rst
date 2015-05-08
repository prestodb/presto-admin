.. _presto-admin-configuration-label:

==========================
Presto Admin Configuration
==========================
A Presto cluster consists of a coordinator node and multiple workers nodes. In order to use ``presto-admin`` to manage software on a cluster of nodes, we must setup a configuration for ``presto-admin`` to know which nodes to install to as well as other credentials in order to communicate with such nodes.

To setup a configuration, create a file ``/etc/opt/presto-admin/config.json`` the content below. Replace the variables denoted with brackets <> with actual values enclosed in quotations. SSH properties ``username`` and ``port`` are optional. If those properties are not included in the ``config.json`` file, then ``root`` and ``22`` will be used by default respectively. The file should be owned by root with R/W permissions (i.e. 622).
::

 {
    "username": "<ssh_user_name>",
    "port": "<ssh_port>",
    "coordinator": "<host_name>",
    "workers": ["<host_name_1>", "<host_name_2>", ... "<host_name_n>"]
 }

E.g. For a 6 node cluster with default username and port, the config.json would be

 {
    "coordinator": "master",
    "workers": ["slave1","slave2","slave3","slave4","slave5"]
 }
