.. _java-installation-label:

===================
Java 8 Installation
===================
Prerequisites: :ref:`presto-admin-installation-label` and :ref:`presto-admin-configuration-label`

The Java 1.8 JRE (64-bit), update 45 or higher, is a prerequisite for Presto. If a suitable 64-bit version of Java 8 is already installed on the cluster, you can skip this step.

The ``presto-admin`` tool simplifies installing Java 8 on all nodes in the Presto cluster. To install the Oracle Java 1.8 (64-bit) RPM using ``presto-admin``, first download `Oracle Java 8 <http://java.com/en/download/>`_ and copy it to a location accessible by ``presto-admin``. Next, run the following command to install Java 8 on each node in the Presto cluster.
::

 $ sudo ./presto-admin package install <local_path_to_java_rpm>

