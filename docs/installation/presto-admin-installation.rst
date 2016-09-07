.. _presto-admin-installation-label:

=======================
Installing Presto Admin
=======================
*Prerequisites:* `Python 2.6 or Python 2.7 <https://www.python.org/downloads>`_

To install ``presto-admin``:

Presto Admin is packaged as an offline installer --
``prestoadmin-<version>-offline.tar.bz2`` -- and as an online
installer -- ``prestoadmin-<version>-online.tar.bz2``. The offline
installer includes all of the dependencies for ``presto-admin``, so it
can be used on a cluster without an outside network connection. The
offline installer is currently only supported on RedHat Linux 6.x or
CentOS equivalent.

The online installer downloads all of the dependencies when you call
``./install-prestoadmin.sh``. You need to use the online installer
for installation of Presto on Amazon EMR.

You may also need to use the online installer if you try to install
``presto-admin`` on an operating system other than listed above,
because some of the binary files are dependent on the system files of
a given operating system. Be aware, though, that there may be other
operating system dependent differences beyond the installation process
for unsupported operating system, and ``presto-admin`` may not work.
 
1. Copy the installer ``prestoadmin-<version>-offline.tar.bz2`` to the
location where you want ``presto-admin`` to run. The recommended
installation location is ``/opt``. Note that ``presto-admin`` does not
have to be on the same node(s) where Presto will run, though it does
need to have SSH access to all of the nodes in the cluster.

.. NOTE::
     For Amazon EMR, use the online installer instead of the offline installer.

.. NOTE::
   Omit the use of ‘sudo’ when the prestoadmin scripts and presto-admin commands when the root user is selected as the prestoadmin user.

.. NOTE::
   The sudo setup for a non-root user has a security hole where the presto-admin non-root user requires the ability to run /bin/bash as root. The IT organization will need to take the appropriate steps to address this security hole and select which user will be the presto-admin user.


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
