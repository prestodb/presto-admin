======================
Upgrading Presto-Admin
======================

Upgrading to a newer version of ``presto-admin`` requires deleting the old
installation and then installing the new version.  The recommended installation
location is ``/opt/prestoadmin``, but you may have installed it in an
alternative location.  After you've deleted the ``prestoadmin`` directory,
install the newer version of ``presto-admin`` by following the instructions in
the installation section (see :ref:`presto-admin-installation-label`). Your
configuration files located in ``/etc/opt/prestoadmin`` will remain intact and
continue to be used by the newer version of ``presto-admin``.
