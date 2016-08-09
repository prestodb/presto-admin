=============
Release 1.4.1
=============

This release works for Presto versions 0.116-0.151.

* Fix server start/stop/status for Presto 0.149+
* Add support to download the Presto server from the internet
* Fix support for various commands for non-root remote users
* Fix permissions that presto-admin requires such that root on the cluster does not need permission to run commands as other groups
