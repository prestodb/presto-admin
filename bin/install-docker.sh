#!/bin/bash -x

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

# Install docker on Ubuntu 14.04
wget -qO- https://get.docker.com/ | sh

# Add current user to Docker group to run without sudo
sudo gpasswd -a ${USER} docker

sudo sh -c "echo 'DOCKER_OPTS=\"--dns 153.65.2.111 --dns 8.8.8.8\"' >> /etc/default/docker"

sudo service docker restart
