.. _java-installation-label:

===================
Java 8 Installation
===================
*Prerequisites: If using* ``presto-admin`` *to install Java 8, it is essential the* :ref:`presto-admin-configuration-label` *step has been completed*

Java 8 is a prerequisite for Presto. If Java 8 is already installed on the cluster, you can skip this step.

``presto-admin`` simplifies installing Java 8 on all nodes in the Presto cluster. To install Java 8 using ``presto-admin``, first download `Oracle Java 8 <http://java.com/en/download/>`_ and copy it to a location accessible by ``presto-admin``. Next, run the following command to install Java 8 on each node in the Presto cluster.
::

 $ sudo ./presto-admin package install <local_path_to_java_rpm>

