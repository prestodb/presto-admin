.. _presto-server-installation-label:

============================
Installing the Presto Server
============================
Prerequisites: :ref:`presto-admin-installation-label`, :ref:`java-installation-label` and :ref:`presto-admin-configuration-label`

To install the Presto query engine on a cluster of nodes using ``presto-admin``:

1. Download ``presto-server-rpm-VERSION.ARCH.rpm``

2. Copy the RPM to a location accessible by ``presto-admin``.

3. Run the following command to install Presto: ::

    $ ./presto-admin server install <local_path_to_rpm>


Presto! Presto is now installed on the coordinator and workers specified in your ``~/.prestoadmin/config.json`` file.

The default port for Presto is 8080.  If that port is already in use on your cluster, you will not be able to start Presto.
In order to change the port that Presto uses, proceed to :ref:`presto-port-configuration-label`.

There are additional configuration properties described at :ref:`presto-configuration-label` that
must be changed for optimal performance. These configuration changes can be done either
before or after starting the Presto server and running queries for the first time, though
all configuration changes require a restart of the Presto servers.

4. Now, you are ready to start Presto: ::

    $ ./presto-admin server start

This may take a few seconds, since the command doesn't exit until ``presto-admin`` verifies that Presto is fully up and ready to receive queries.
