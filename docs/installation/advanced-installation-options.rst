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
