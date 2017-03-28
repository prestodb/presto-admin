.. _presto-admin-on-emr-label:
..
.. If you modify this file, you will have to modify the NOTEs in the following files:
.. docs/installation/java-installation.rst
.. docs/installation/presto-admin-configuration.rst
.. docs/installation/presto-admin-installation.rst
..

================================================
Setting up Presto Admin on an Amazon EMR cluster
================================================

To install, configure and run Presto Admin on an Amazon EMR cluster, follow the instructions in :ref:`quick-start-guide-label`, but pay attention to the notes or sections specfic to EMR cluster. We reiterate these EMR specific caveats below:

- To install Presto Admin on an Amazon EMR cluster, follow the instructions in :ref:`presto-admin-installation-label` except for the following difference:

	- Use the online installer instead of the offline installer (see explanation :ref:`presto-admin-installation-label`).

- To configure Presto Admin on an Amazon EMR cluster, follow the instructions in :ref:`presto-admin-configuration-label`. Specifically, we recommend the following property values during the configuration:

	- Use ``hadoop`` as the ``username`` instead of the default username ``root`` in the ``config.json`` file.

	- Use the host name of the EMR master node as the ``coordinator`` in the ``config.json`` file.

- To run Presto Admin on EMR, see the sections starting from :ref:`presto-server-installation-label` onwards in :ref:`quick-start-guide-label`) except for the following caveats:

        - The default version of Java installed on an EMR cluster (up to EMR 4.4.0) is 1.7, whereas Presto requires Java 1.8. Install Java 1.8 on the EMR cluster by following the instructions in :ref:`java-installation-label`.

        - For running Presto Admin commands on an EMR cluster, do the following:
                * Copy the ``.pem`` file associated with the Amazon EC2 key pair to the Presto Admin installation node of the EMR cluster.
                * Use the ``-i <path to .pem file>`` input argument when running presto-admin commands on the node.
		  ::

		   </path/to/presto-admin> -i </path/to/your.pem> <presto_admin_command>
