===========
Release 2.2
===========

New Features
------------
* Support specifying a range of workers in ``config.json``

Bug Fixes and Enhancements
--------------------------
* Fix error with getting server status for complex Presto version names
* Preserve all of ``/etc/presto`` during upgrade
* Use ``rpm -U`` for ``package upgrade`` and ``server upgrade`` instead of uninstalling and reinstalling fresh
* Use ``.gz`` instead of ``.bz2`` for the installation tarballs and for the files collected by ``collect logs``
  and ``collect system_info``

