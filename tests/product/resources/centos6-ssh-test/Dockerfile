FROM jdeathe/centos-ssh
MAINTAINER Christina Wallin <christina.wallin@teradata.com>

# Install Oracle Java
RUN yum update -y && \
    yum install -y wget && \
    wget --header "Cookie: oraclelicense=accept-securebackup-cookie" https://edelivery.oracle.com/otn-pub/java/jdk/8u40-b26/jdk-8u40-linux-x64.rpm
RUN rpm -ivh jdk-8u40-linux-x64.rpm && rm jdk-8u40-linux-x64.rpm

# Install dependencies for presto-admin -- already has python-2.6
RUN yum -y install bzip2 \
    gcc \
    python-devel \
    tar

# Copy Vagrant insecure SSH keys for passwordless SSH between
# containers and to localhost
RUN curl -LsSO https://raw.githubusercontent.com/mitchellh/vagrant/master/keys/vagrant && \
  mv vagrant /etc/services-config/ssh/id_rsa

# Modify the ssh-bootstrap script to copy id_rsa to ~/.ssh (the script already makes ~/.ssh
# and copies authorized_keys)
RUN printf " if [[ -e /etc/services-config/ssh/id_rsa ]]; then \n \ 
            cp -f /etc/services-config/ssh/id_rsa \${OPTS_SSH_USER_HOME_DIR}/.ssh/id_rsa\n \
	    chown -R \${OPTS_SSH_USER}:${OPTS_SSH_USER} \${OPTS_SSH_USER_HOME_DIR}/.ssh/id_rsa\n \
	    chmod 600 \${OPTS_SSH_USER_HOME_DIR}/.ssh/id_rsa\n \
	  fi\n\
	  " > /etc/bootstrap-extra

RUN awk -v file="/etc/bootstrap-extra" -v lineno=$(($(wc -l < /etc/ssh-bootstrap) - 3)) ' \
    NR==lineno {system("echo ""; cat " file); print; next} \
    1' /etc/ssh-bootstrap > /etc/ssh-bootstrap2 && \
    mv /etc/ssh-bootstrap2 /etc/ssh-bootstrap && \
    chmod +x /etc/ssh-bootstrap

# Set default password for testing
RUN printf "\nSSH_USER_PASSWORD=password" >> /etc/services-config/ssh/ssh-bootstrap.conf

EXPOSE 22 8080

# Clean up
RUN yum -y clean all && rm -rf /tmp/* /var/tmp/*
