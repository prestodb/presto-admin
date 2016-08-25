.. _presto-admin-installation-label:

=======================
Installing Presto Admin
=======================
*Prerequisites:* `Python 2.6 or Python 2.7 <https://www.python.org/downloads>`_

.. NOTE::
     For Amazon EMR, use the online installer instead of the offline installer (see :ref:`advanced-installation-options-label`).

.. NOTE::
   Omit the use of ‘sudo’ when the prestoadmin scripts and presto-admin commands when the root user is selected as the prestoadmin user.

.. NOTE::
   The sudo setup for a non-root user has a security hole where the presto-admin non-root user requires the ability to run /bin/bash as root. The IT organization will need to take the appropriate steps to address this security hole and select which user will be the presto-admin user.
     
To install ``presto-admin``:
 
1. Copy the installer ``prestoadmin-1.4-SNAPSHOT-offline.tar.bz2`` to the location where you want
``presto-admin`` to run. The recommended installation location is ``/opt``. Note that ``presto-admin`` does not have to be on the same node(s) where Presto will run, though it does need to have SSH access to all of the nodes in the cluster.

2. Extract and sudo run the installation script from within the ``prestoadmin`` directory.
::

 $ tar xvf prestoadmin-1.4-SNAPSHOT-offline.tar.bz2
 $ cd prestoadmin
 $ sudo ./install-prestoadmin.sh

The installation script will create a ``presto-admin-install`` directory and an
executable ``presto-admin`` script. Make sure to run the installation script
with sudo when the presto-admin user is non-root.

3. Verify that ``presto-admin`` was installed properly by running the
``presto-admin`` help.  Please note that it is necessary to run all
``presto-admin`` commands with sudo:
::

 $ sudo ./presto-admin --help

Please note that you should only run one ``presto-admin`` command on your
cluster at a time.
