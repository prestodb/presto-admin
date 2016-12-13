===========
Release 2.0
===========

New Features
------------
* Make presto-admin log and configuration directories configurable. They can be
  set using the environment variables ``PRESTO_ADMIN_LOG_DIR`` and
  ``PRESTO_ADMIN_CONFIG_DIR``.
* Change the default configuration directory to ``~/.prestoadmin`` and the
  default log directory to ``~/.prestoadmin/log``.
* Remove the requirement for running and installing presto-admin with sudo.
  The user specified in ``config.json`` still needs sudo access on the Presto
  nodes in order to execute commands like installing the RPM and setting
  permissions on the configuration files.
* Rename the ``connectors`` directory to ``catalog`` to match the Presto
  nomenclature.
* Rename the ``connector add`` and ``connector remove``. commands to
  ``catalog add`` and ``catalog remove``.
* Add experimental support for connecting to a Presto server with internal
  communication via HTTPS and LDAP, where the HTTP connection is disabled.
* Allow specifying which python interpreter to use as an argument to the
  presto-admin installation script.
* Add ``G1HeapRegionSize=32M`` to the jvm.config defaults as suggested by the
  Presto documentation.

Bug Fixes
---------
* Keep the ``node.id`` in Presto's ``node.properties`` file consistent across
  configuration updates.
* Change the permissions on the Presto catalog directory to ``755`` and the
  owner to``presto:presto``.
* Use ``catalog.config-dir`` instead of ``plugin.config-dir`` in the
  ``node.properties`` defaults. ``plugin.config-dir`` has been deprecated
  in Presto since version 0.113.

Compatibility Notes
-------------------
* The locations of config and log directories have been changed
* The ``connectors`` directory has been renamed to ``catalog``.
* The ``connector`` commands have been renamed to ``catalog``.
