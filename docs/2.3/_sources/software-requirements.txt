=====================
Software Requirements
=====================

**Operating Systems**
* RedHat Linux version 6.x		
* CentOS (equivalent to above)

**Python**

* Python 2.6.x OR
* Python 2.7.x

**SSH Configuration**

* Passwordless SSH from the node running ``presto-admin`` to the nodes where Presto will be installed OR
* Ability to SSH with a password from the node running ``presto-admin`` to the nodes where Presto will be installed

For more on SSH configuration, see :ref:`ssh-configuration-label`.

**Other Configuration**

* Sudo privileges on both the node running ``presto-admin`` and the nodes where Presto will be installed are required for a non-root presto-admin user.
