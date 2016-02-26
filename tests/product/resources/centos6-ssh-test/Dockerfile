# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM jdeathe/centos-ssh:centos-6-1.2.0
MAINTAINER Christina Wallin <christina.wallin@teradata.com>

# Install Oracle Java and presto-admin dependences (already has python 2.6)
RUN yum upgrade ca-certificates --disablerepo=epel -y && \
    yum update -y && \
    yum install -y wget bzip2 gcc python-devel tar expect && \
    wget --header "Cookie: oraclelicense=accept-securebackup-cookie" https://edelivery.oracle.com/otn-pub/java/jdk/8u40-b26/jdk-8u40-linux-x64.rpm
RUN rpm -ivh jdk-8u40-linux-x64.rpm && rm jdk-8u40-linux-x64.rpm

# Copy Vagrant insecure SSH keys for passwordless SSH between
# containers and to localhost.
COPY vagrant_insecure_rsa /etc/services-config/ssh/id_rsa

# Modify the ssh-bootstrap script to copy id_rsa to ~/.ssh (the script already makes ~/.ssh
# and copies authorized_keys)
RUN printf "cp -f /etc/services-config/ssh/id_rsa \${OPTS_SSH_USER_HOME_DIR}/.ssh/id_rsa\n \
	    chown -R \${OPTS_SSH_USER}:${OPTS_SSH_USER} \${OPTS_SSH_USER_HOME_DIR}/.ssh/id_rsa\n \
	    chmod 600 \${OPTS_SSH_USER_HOME_DIR}/.ssh/id_rsa\n \
	    cp -r \${OPTS_SSH_USER_HOME_DIR}/.ssh /root/.ssh\n" > /etc/bootstrap-extra

RUN printf "\nuseradd -ms /bin/bash testuser\necho "testuser:testpass" | chpasswd" >> /etc/bootstrap-extra

RUN awk -v file="/etc/bootstrap-extra" -v lineno=$(($(wc -l < /etc/ssh-bootstrap) - 3)) ' \
    NR==lineno {system("echo ""; cat " file); print; next} \
    1' /etc/ssh-bootstrap > /etc/ssh-bootstrap2 && \
    mv /etc/ssh-bootstrap2 /etc/ssh-bootstrap && \
    chmod +x /etc/ssh-bootstrap

RUN sed -i \
    -e 's/^PermitRootLogin no/PermitRootLogin without-password/g' \
    -e 's/^PasswordAuthentication no/PasswordAuthentication yes/g' \
    /etc/ssh/sshd_config

# Set default password for testing
RUN printf "\nSSH_USER_PASSWORD=password" >> /etc/services-config/ssh/ssh-bootstrap.conf

EXPOSE 22 8080

# Clean up
RUN yum -y clean all && rm -rf /tmp/* /var/tmp/*
