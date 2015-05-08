.. _presto-admin-installation-label:

=========================
Presto Admin Installation
=========================
*Prerequisites:* `Python 2.6 or Python 2.7 <https://www.python.org/downloads>`_


To install presto-admin, first download `prestoadmin-0.1.0.tar.bz2 <https://jenkins-master.td.teradata.com/view/Presto/job/presto-admin/lastSuccessfulBuild/artifact/presto-admin/dist/prestoadmin-0.1.0.tar.bz2>`_ and copy it to the location where you want presto-admin to run. It does not have to be on same machine(s) where Presto will run. Next, extract and sudo run the installation script from within the presto-admin directory.
::

 $ tar xvf prestoadmin-0.1.0.tar.bz2
 $ cd prestoadmin
 $ sudo ./install-prestoadmin.sh

Or you can also download using wget
::

 $ wget https://jenkins-master.td.teradata.com/view/Presto/job/presto-admin/lastSuccessfulBuild/artifact/presto-admin/dist/prestoadmin-0.1.0.tar.bz2


The installation script will create a ``presto-admin-install`` directory and an executable ``presto-admin`` script.
You can verify ``presto-admin`` was installed by running the ``presto-admin`` help.
::

 $ sudo ./presto-admin --help

