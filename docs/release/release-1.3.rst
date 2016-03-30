===========
Release 1.3
===========

The default values in this release are intended to work with Presto versions
0.116 through x. However, the user can supply non-default
configurations to use this release with other versions of Presto.

General Fixes
-------------
* Change ``make dist`` to build the online installer by default 
* Configuration files deployed to the coordinator and workers are owned by
  ``presto`` and are no longer readable by anybody else. This prevents
  unauthorized users from gaining access to e.g. plaintext passwords in connector
  configuration files.
