#!/bin/bash -x

# Install docker on Ubuntu 14.04
wget -qO- https://get.docker.com/ | sh

# Add current user to Docker group to run without sudo
sudo gpasswd -a ${USER} docker
newgrp docker

sudo sh -c "echo 'DOCKER_OPTS=\"--dns 153.65.2.111 --dns 8.8.8.8\"' >> /etc/default/docker"

sudo service docker start
