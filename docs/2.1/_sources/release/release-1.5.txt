===========
Release 1.5
===========

This release works for Presto versions 0.116 through at least 0.152.1

New Features
------------
* Add the ability to download the rpm in ``server install`` by specifying ``latest`` or a version number
* Add a ``file copy`` command to distribute files to all nodes on the cluster
* Collect connector configurations from each node as part of ``collect system_info``

Bug Fixes
---------
* Fix a bug where a non-root user in ``config.json`` could not access files

Compatiblity Notes
------------------
* The ``script run`` command was renamed to ``file run``
