.. _presto-admin-installation-label:

=========================
Presto Admin Installation
=========================
*Prerequisites:* `Python 2.6 or Python 2.7 <https://www.python.org/downloads>`_


To install ``presto-admin``, first download `prestoadmin-0.1.0.tar.bz2 <TODO: ADD URL HERE>`_ and copy it to the location where you want ``presto-admin`` to run. The recommended installation location is ``/opt``. Note that ``presto-admin`` does not have to be on same node(s) where Presto will run, though it does need to have SSH access to all of them. Next, extract and sudo run the installation script from within the ``prestoadmin`` directory.
::

 $ tar xvf prestoadmin-0.1.0.tar.bz2
 $ cd prestoadmin
 $ sudo ./install-prestoadmin.sh

The installation script will create a ``presto-admin-install`` directory and an executable ``presto-admin`` script. Make sure to run the installation script with sudo.
You can verify ``presto-admin`` was installed by running the ``presto-admin`` help.  Please note that it is necessary to run all ``presto-admin`` commands with sudo.
::

 $ sudo ./presto-admin --help


For troubleshooting installation and other advanced installation options, see :ref:`advanced-installation-options-label`.