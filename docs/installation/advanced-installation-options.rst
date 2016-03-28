.. _advanced-installation-options-label:

=============================
Advanced Installation Options
=============================

Online vs Offline Installer
---------------------------
The installer has two different versions: the offline installer -- 
``prestoadmin-1.4-SNAPSHOT-offline.tar.bz2``-- and the online installer --
``prestoadmin-1.4-SNAPSHOT-online.tar.bz2``. The offline installer includes all of the
dependencies for ``presto-admin``, so it can be used on a cluster without an 
outside network connection. The offline installer is recommended because it is faster.

The online installer downloads all of the dependencies when you call ``./install-prestoadmin.sh``. 
You may need to use the online installer if you try to install ``presto-admin`` on an unsupported 
operating system, because some of the binary files are dependent on the 
system files of a given operating system. Be aware, though, that there may be other 
operating system dependent differences beyond the installation process for 
unsupported operating system, and ``presto-admin`` may not work.

For instructions on how to build either installer see the
`README <https://github.com/prestodb/presto-admin>`_ in the presto-admin
repository.

Specifying a Certificate Authority for the Online Installer
-----------------------------------------------------------
The online installer downloads its dependencies from ``pypi.python.org``, the 
standard Python location for hosting packages. For some operating systems, 
the certificate for pypi.python.org is not included in the CA cert bundle, 
so our installation scripts specify ``--trusted-host pypi.python.org`` when 
downloading the dependencies.

If using ``--trusted-host`` is not suitable for your security needs, it is
possible to supply your own certificates to use to authenticate to 
``pypi.python.org``.  Please note that if these certificates do not work to 
access ``pypi.python.org``, the installation will fail. For example, to install 
with your own certificates:

::

 sudo ./install-prestoadmin.sh <path_to_cert>/cacert.pem

Upgrading Presto-Admin
----------------------
Upgrading to a newer version of ``presto-admin`` requires deleting the old
installation and then installing the new version.  The recommended installation
location is ``/opt/prestoadmin``, but you may have installed it in an
alternative location.  After you've deleted the ``prestoadmin`` directory,
install the newer version of ``presto-admin`` by following the instructions in
the installation section (see :ref:`presto-admin-installation-label`). Your
configuration files located in ``/etc/opt/prestoadmin`` will remain intact and
continue to be used by the newer version of ``presto-admin``.
