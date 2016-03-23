.. _java-installation-label:

=================
Installing Java 8
=================
Prerequisites: :ref:`presto-admin-installation-label` and :ref:`presto-admin-configuration-label`

The Oracle Java 1.8 JRE (64-bit), update 45 or higher, is a prerequisite for Presto. If a suitable 64-bit version of Oracle Java 8 is already installed on the cluster, you can skip this step.

There are two ways to install Java: via RPM and via tarball.  The RPM installation sets the default Java on your machine to be Java 8. If 
it is acceptable to set the default Java to be Java 8, you can use ``presto-admin`` to install Java, otherwise you will need to install Java 8 manually.

To install Java via RPM using ``presto-admin``:
 
1. Download `Oracle Java 8 <http://java.com/en/download/linux_manual.jsp>`_, selecting the Oracle Java 1.8 (64-bit) RPM download for Linux.

2. Copy the RPM to a location accessible by ``presto-admin``.

3. Run the following command to install Java 8 on each node in the Presto cluster: ::

    $ sudo ./presto-admin package install <local_path_to_java_rpm>


.. NOTE:: The ``server-install-label`` will look for your Oracle Java 1.8 installation at locations where Java is normally installed when using the binary or the RPM based installer. Otherwise, you need to have an environment variable called ``JAVA8_HOME`` set with your Java 1.8 install path. If ``JAVA8_HOME`` is not set or is pointing to an incompatible version of Java, the installer will look for the ``JAVA_HOME`` environment variable for a compatible version of Java. If neither of these environmental variables is set with a compatible version, and ``presto-admin`` fails to find Java 8 at any of the normal installation locations, then ``server install`` will fail. After successfully running ``server install`` you can find the Java being used by Presto at ``/etc/presto/env.sh``.

.. NOTE:: If installing Java on SLES, you will need to specify the flag ``--nodeps`` for ``presto-admin package install``, so that the RPM is installed without checking or validating dependencies.
