===========
Release 1.2
===========

The default values in this release are intended to work with Presto versions
0.116 through at least 0.130. However, the user can supply non-default
configurations to use this release with other versions of Presto.

General Fixes
-------------
* Fix server status to work with later versions of Presto
* Exit with non-zero code when operations fail
* Update configuration defaults for Presto versions >0.115
* Make remote log directory configurable
* Add support for specifying java8 home in config.json
* :ref:`collect-logs` will use the log directory specified in
  Presto's config.properties if configured.


Configuration
-------------
Before this release, :ref:`configuration-deploy-label` would fill in default
values for any required properties that the user did not supply in the
configuration files. However, this created problems when different versions of
Presto had different configuration requirements.  In particular, it became
impossible to remove any required properties from the configuration even if the
user's Presto version did not require those properties.

In the current behavior, when the user needs to override the defaults in any
configuration file, they must write out all the properties for that
configuration file, which will be deployed as-is.
