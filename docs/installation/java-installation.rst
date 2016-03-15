.. _java-installation-label:

===================
Java 8 Installation
===================
Prerequisites: :ref:`presto-admin-installation-label` and :ref:`presto-admin-configuration-label`

The Oracle Java 1.8 JRE (64-bit), update 45 or higher, is a prerequisite for Presto. If a suitable 64-bit version of Oracle Java 8 is already installed on the cluster, you can skip this step.

The simplest way to install Java 8 on all nodes in the Presto cluster is to use ``presto-admin``. To install the Oracle Java 1.8 (64-bit) RPM for Linux using ``presto-admin``, first download `Oracle Java 8 <http://java.com/en/download/linux_manual.jsp>`_ and copy it to a location accessible by ``presto-admin``. Next, run the following command to install Java 8 on each node in the Presto cluster.
::

 $ sudo ./presto-admin package install <local_path_to_java_rpm>

.. NOTE:: Using this method of installation will cause the default Java on your machine to be Java 8. If that is not desirable, you must download the tarball version of Java 8 and install it manually.

.. NOTE:: The ``server-install-label`` will look for your Oracle Java 1.8 installation at locations where Java is installed normally using the binary or the RPM based installer. Otherwise, you need to have an environment variable called ``JAVA8_HOME`` set with your Java 1.8 install path. If ``JAVA8_HOME`` is not set or is pointing to an incompatible version of Java, the installer will look for the ``JAVA_HOME`` environment variable for a compatible version of Java. If neither of these environmental variables is set with a compatible version, and ``presto-admin`` fails to find Java 8 at any of the normal install locations, then ``server install`` will fail. After successfully running ``server install`` you can find the Java being used by Presto at ``/etc/presto/env.sh``.

.. NOTE:: If installing Java on SLES, you may need to specify the ``--nodeps`` flag, so that the RPM is installed without checking or validating dependencies.
