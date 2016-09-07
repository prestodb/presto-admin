=============================
Advanced Installation Options
=============================

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
