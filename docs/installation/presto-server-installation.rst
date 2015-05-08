==========================
Presto Server Installation
==========================
Prerequisites: :ref:`presto-admin-installation-label` and :ref:`java-installation-label`

The following describes how to install the Presto server on a cluster of machines using the presto-admin software utility.

First download `presto-0.101-1.0.x86_64.rpm <https://jenkins-master.td.teradata.com/view/Presto/job/presto-td/1975/artifact/presto-server/target/rpm/presto/RPMS/x86_64/presto-0.101-1.0.x86_64.rpm>`_ and copy it to a location accessible by ``presto-admin``. Then to install Presto, run the following.
::

 $ sudo ./presto-admin server install <local_path_to_rpm>

You can also download using wget
::

 $ wget https://jenkins-master.td.teradata.com/view/Presto/job/presto-td/1975/artifact/presto-server/target/rpm/presto/RPMS/x86_64/presto-0.101-1.0.x86_64.rpm

Presto! Presto is now installed. Before you can issue your first query, you must start it and set up the :ref:`presto-cli-installation-label`
::

 $ sudo ./presto-admin server start
