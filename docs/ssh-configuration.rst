.. _ssh-configuration-label:

*****************
SSH Configuration
*****************

In order to run ``presto-admin``, the node that is running ``presto-admin`` must be able to connect to all of the nodes running Presto via SSH. ``presto-admin`` makes the SSH connection with the username and port specified in ``~/.prestoadmin/config.json``. Even if you have a single-node installation, ``ssh username@localhost`` needs to work properly.

There are two ways to configure SSH: with keys so that you can use passwordless SSH, or with passwords. If your cluster already has passwordless SSH configured for the username ``user``, you can skip this step if the username is root, otherwise the root public key (id_rsa.pub) needs to be appended to the non-root username’s authorized_keys file. If you are intending to use ``presto-admin`` with passwords, take a look at the documentation below, because there are several ways to specify the password.

Using ``presto-admin`` with passwordless SSH
--------------------------------------------
In order to set up passwordless SSH, you must first login as username on the presto-admin node and generate keys with no passphrase on the node running ``presto-admin``:
::

 ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

While logged in as username, copy the public key to all of the coordinator and worker nodes:
::

 ssh <username>@<ip> "mkdir -p ~/.ssh && chmod 700 ~/.ssh"
 scp ~/.ssh/id_rsa.pub <username>@<ip>:~/.ssh/id_rsa.pub

Log into all of those nodes and append the public key to the authorized key file:
::

 ssh <username>@<ip> "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"

For non-root username, log into all of those nodes and append the root user public key to the username authorized key file, provided the passwordless ssh has been setup for root user.:
::

   ssh <username>@<ip> "sudo cat /root/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"

Once you have passwordless SSH set up, you can just run ``presto-admin`` commands as they appear in the documentation. If your private key is not in ``~/.ssh``, it is possible to specify one or several private keys using the -i CLI option:

::

 ./presto-admin <command> -i <path_to_private_key> -i <path_to_second_private_key>


Please also note that it is not common for servers to allow passwordless SSH for root because of security concerns, so it is preferable for the SSH user not to be root.

Using ``presto-admin`` with SSH passwords
-----------------------------------------
If you do not want to set up passwordless SSH on your cluster, it is possible to use ``presto-admin`` with SSH passwords. However, you will need to add a password argument to the ``presto-admin`` commands as they appear in the documentation. There are several options. To specify a password on the CLI in plaintext:

::

 ./presto-admin <command> -p <password>

However, from a security perspective, it is preferable not to type your password in plaintext. Thus, it is also possible to add an interactive password prompt, which prompts you for the initial value of your password before running any commands:

::

 ./presto-admin <command> -I
 Initial value for env.password: <type your password here>

If you do not specify a password, the command will fail with a parallel execution failure, since, by default, ``presto-admin`` runs in parallel and cannot prompt for a password while running in parallel. If you specify the ``--serial`` option for ``presto-admin``, ``presto-admin`` will prompt you for a password if it cannot connect.

Please note that the SSH password for the user specified in ``~/.prestoadmin/config.json`` must match the sudo password for that user.

