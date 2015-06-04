.. _java-installation-label:

===================
Java 8 Installation
===================
Prerequisites: :ref:`presto-admin-installation-label` and :ref:`presto-admin-configuration-label`

The Oracle Java 1.8 JRE (64-bit), update 45 or higher, is a prerequisite for Presto. If a suitable 64-bit version of Oracle Java 8 is already installed on the cluster, you can skip this step.

The ``presto-admin`` tool simplifies installing Java 8 on all nodes in the Presto cluster. To install the Oracle Java 1.8 (64-bit) RPM for Linux using ``presto-admin``, first download `Oracle Java 8 <http://java.com/en/download/linux_manual.jsp>`_ and copy it to a location accessible by ``presto-admin``. Next, run the following command to install Java 8 on each node in the Presto cluster.
::

 $ sudo ./presto-admin package install <local_path_to_java_rpm>

.. NOTE:: The ``server-install-label`` will look for your Oracle Java 1.8 installation at locations where Java is installed normally using the binary or the RPM based installer. Otherwise, you need to have your ``JAVA_HOME`` environment variable set for ``presto-admin`` to find it. If ``presto-admin`` fails to find Java at the normal install locations or if ``JAVA_HOME`` is set with an incompatible Java version, then ``server install`` will fail. After successfully running ``server install`` you can find the Java being used by Presto at ``/etc/presto/env.sh``.

