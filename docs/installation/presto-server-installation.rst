==========================
Presto Server Installation
==========================
Prerequisites: :ref:`presto-admin-installation-label`, :ref:`java-installation-label` and :ref:`presto-admin-configuration-label`

The following describes how to install the Presto query engine on a cluster of nodes using the ``presto-admin`` software utility.

First download the `presto-server-rpm-VERSION.ARCH.rpm` and copy it to a location accessible by ``presto-admin``. Then, to install Presto, run the following:
::

 $ sudo ./presto-admin server install <local_path_to_rpm>


Presto! Presto is now installed on the coordinator and workers specified in your ``/etc/opt/prestoadmin/config.json`` file. 

The default port for Presto is 8080.  If that port is already in use on your cluster, you will not be able to start Presto.
In order to change the port that Presto uses, and if necessary check what ports are in use, proceed to :ref:`presto-port-configuration-label`.

Now, you are ready to start Presto:

::

 $ sudo ./presto-admin server start

This may take a few seconds, since the command doesn't exit until ``presto-admin`` verifies that Presto is fully up and ready to receive queries.
