.. _presto-admin-installation-label:

=======================
Installing Presto Admin
=======================
*Prerequisites:* `Python 2.6 or Python 2.7 <https://www.python.org/downloads>`_


To install ``presto-admin``:
 
1. Copy the installer ``prestoadmin-1.4-SNAPSHOT-offline.tar.bz2`` to the location where you want
``presto-admin`` to run. The recommended installation location is ``/opt``.


.. NOTE:: 
    ``presto-admin`` does not have to be on the same node(s) where Presto will run,
    though it does need to have SSH access to all of the nodes in the cluster.

2. Extract and sudo run the installation script from within the ``prestoadmin`` directory.
::

 $ tar xvf prestoadmin-1.4-SNAPSHOT-offline.tar.bz2
 $ cd prestoadmin
 $ sudo ./install-prestoadmin.sh

The installation script will create a ``presto-admin-install`` directory and an
executable ``presto-admin`` script. Make sure to run the installation script
with sudo.

3. Verify that ``presto-admin`` was installed properly by running the
``presto-admin`` help.  Please note that it is necessary to run all
``presto-admin`` commands with sudo:
::

 $ sudo ./presto-admin --help

Please note that you should only run one ``presto-admin`` command on your
cluster at a time.
