======================
Upgrading Presto-Admin
======================

Upgrading to a newer version of ``presto-admin`` requires deleting the old
installation and then installing the new version.  After you've deleted the
``prestoadmin`` directory, install the newer version of ``presto-admin``
by following the instructions in the installation section
(see :ref:`presto-admin-installation-label`).

For ``presto-admin`` versions earlier than 2.0, the configuration files are
located at ``/etc/opt/prestoadmin``.  To upgrade to a newer version and
continue to use these configuration files, make sure you copy them to the
new configuration directory at ``~/.prestoadmin`` (or
``$PRESTO_ADMIN_CONFIG_DIR``). The connector configuration directory
located at ``/etc/opt/prestoadmin/connectors`` must be renamed to
``/etc/opt/prestoadmin/catalog``, before copying to ``~/.prestoadmin``.

For ``presto-admin`` versions 2.0 and later, the configuration files
located in ``~/.prestoadmin`` will remain intact and continue to be used
by the newer version of ``presto-admin``.
