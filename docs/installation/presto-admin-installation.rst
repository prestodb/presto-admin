.. _presto-admin-installation-label:

=======================
Installing Presto Admin
=======================

Prerequisites:
 - `Python 2.6 or Python 2.7 <https://www.python.org/downloads>`_.
 - If you are using the online installer then make sure you've installed the
   Python development package for your system. For RedHat/Centos that package is
   ``python2-devel`` and for Debian/Ubuntu it is ``python-dev``.

Presto Admin is packaged as an offline installer --
``prestoadmin-<version>-offline.tar.gz`` -- and as an online
installer -- ``prestoadmin-<version>-online.tar.gz``.

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

1. Copy the installer ``prestoadmin-<version>-offline.tar.gz`` to the
location where you want ``presto-admin`` to run.
Note that ``presto-admin`` does not have to be on the same node(s)
where Presto will run, though it does need to have SSH access to all
of the nodes in the cluster.

.. NOTE::
     For Amazon EMR, use the online installer instead of the offline installer.

2. Extract and run the installation script from within the ``prestoadmin`` directory.
::

 $ tar xvf prestoadmin-<version>-offline.tar.gz
 $ cd prestoadmin
 $ ./install-prestoadmin.sh

The installation script will create a ``presto-admin-install`` directory and an
executable ``presto-admin`` script. By default, the ``presto-admin`` config and log
directory locations are configured to be ``~/.prestoadmin`` and ``~/.prestoadmin/log``,
respectively.  This can be changed by modifying the environment variables,
PRESTO_ADMIN_CONFIG_DIR and PRESTO_ADMIN_LOG_DIR. The installation script will also create
the directories pointed to by PRESTO_ADMIN_CONFIG_DIR and PRESTO_ADMIN_LOG_DIR. If those
directories already exist, the installation script will not erase their contents.

3. Verify that ``presto-admin`` was installed properly by running the following
command:
::

 $ ./presto-admin --help

Please note that you should only run one ``presto-admin`` command on your
cluster at a time.
