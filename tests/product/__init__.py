# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import os

from exceptions import Exception


def determine_jdk_directory(cluster):
    """
    Return the directory where the JDK is installed. For example if the JDK is
    located in /usr/java/jdk1.8_91, then this method will return the string
    'jdk1.8_91'.

    This method will throw an Exception if the number of JDKs matching the
    /usr/java/jdk* pattern is not equal to 1.

    :param cluster: cluster on which to search for the JDK directory
    """
    number_of_jdks = cluster.exec_cmd_on_host(cluster.master, 'bash -c "ls -ld /usr/java/j*| wc -l"')
    if int(number_of_jdks) != 1:
        raise Exception('The number of JDK directories matching /usr/java/jdk* is not 1')
    output = cluster.exec_cmd_on_host(cluster.master, 'ls -d /usr/java/j*')
    return output.split(os.path.sep)[-1].strip('\n')


@contextlib.contextmanager
def relocate_jdk_directory(cluster, destination):
    """
    Temporarily move the JDK to the destination directory

    :param cluster: cluster object on which to relocate the JDK directory
    :param destination: destination parent JDK directory, e.g. /tmp/
    :returns the new full JDK directory, e.g. /tmp/jdk1.8_91
    """
    # assume that Java is installed in the same folder on all nodes
    jdk_directory = determine_jdk_directory(cluster)
    source_jdk = os.path.join('/usr/java', jdk_directory)
    destination_jdk = os.path.join(destination, jdk_directory)
    for host in cluster.all_hosts():
        cluster.exec_cmd_on_host(
            host, "mv %s %s" % (source_jdk, destination_jdk), invoke_sudo=True)

    yield destination_jdk

    for host in cluster.all_hosts():
        cluster.exec_cmd_on_host(
            host, "mv %s %s" % (destination_jdk, source_jdk), invoke_sudo=True)
