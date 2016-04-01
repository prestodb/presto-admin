=====================
Software Requirements
=====================

**Operating System**

* RedHat Linux version 6.x
* CentOS (equivalent to above)

**Hadoop distributions**

* HDP 2.x OR
* CDH 5.x

**Java**

* Oracle Java 1.8 JRE (64-bit) (Required for Presto. See :ref:`java-installation-label`)

**Python**

* Python 2.6.x OR
* Python 2.7.x

**SSH Configuration**

* Passwordless SSH between the node running ``presto-admin`` and the nodes where Presto will be installed OR
* Ability to SSH with a password between the node running ``presto-admin`` and the nodes where Presto will be installed

For more on SSH configuration, see :ref:`ssh-configuration-label`.

**Other Configuration**

* Sudo privileges on both the node running ``presto-admin`` and the nodes where Presto will be installed
