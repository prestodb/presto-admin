============================
Installing the Presto Server
============================
Prerequisites: :ref:`presto-admin-installation-label`, :ref:`java-installation-label` and :ref:`presto-admin-configuration-label`

To install the Presto query engine on a cluster of nodes using ``presto-admin``:

1. Download ``presto-server-rpm-VERSION.ARCH.rpm``

2. Copy the RPM to a location accessible by ``presto-admin``.

3. Run the following command to install Presto: ::

    $ sudo ./presto-admin server install <local_path_to_rpm>


Presto! Presto is now installed on the coordinator and workers specified in your ``/etc/opt/prestoadmin/config.json`` file. 

The default port for Presto is 8080.  If that port is already in use on your cluster, you will not be able to start Presto.
In order to change the port that Presto uses, proceed to :ref:`presto-port-configuration-label`.

4. Now, you are ready to start Presto: ::

    $ sudo ./presto-admin server start

This may take a few seconds, since the command doesn't exit until ``presto-admin`` verifies that Presto is fully up and ready to receive queries.
