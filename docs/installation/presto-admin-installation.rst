.. _presto-admin-installation-label:

=======================
Installing Presto Admin
=======================
*Prerequisites:* `Python 2.6 or Python 2.7 <https://www.python.org/downloads>`_

Presto Admin is packaged as an offline installer --
``prestoadmin-<version>-offline.tar.bz2`` -- and as an online
installer -- ``prestoadmin-<version>-online.tar.bz2``.

The offline installer includes all of the dependencies for
``presto-admin``, so it can be used on a cluster without an outside
network connection. The offline installer is currently only supported
on RedHat Linux 6.x or CentOS equivalent.

The online installer downloads all of the dependencies when you run
``./install-prestoadmin.sh``. You must use the online installer for
installation of Presto on Amazon EMR and for use on any operating
system not listed above. If you are using presto-admin on an
unsupported operating system, there may be operating system
dependencies beyond the installation process, and presto-admin may not
work.

To install ``presto-admin``:

1. Copy the installer ``prestoadmin-<version>-offline.tar.bz2`` to the
location where you want ``presto-admin`` to run. The recommended
installation location is ``/opt``. Note that ``presto-admin`` does not
have to be on the same node(s) where Presto will run, though it does
need to have SSH access to all of the nodes in the cluster.

.. NOTE::
     For Amazon EMR, use the online installer instead of the offline installer.
   
2. Extract and sudo run the installation script from within the ``prestoadmin`` directory.
::

 $ tar xvf prestoadmin-1.5-offline.tar.bz2
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
