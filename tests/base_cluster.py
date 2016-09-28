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

"""
Abstract base class for clusters

BaseCluster defines the minimum set of methods that a cluster needs to
implement in order to be useful.
"""

import abc
import sys

from tests.product import determine_jdk_directory


class BaseCluster(object):
    """
    Besides the instance methods defined here, clusters typically have a static
    factory method that hides some of the complexity of bringing a bare cluster
    into existence. The parameters to this method vary greatly depending on the
    nature of the implementation, and so it doesn't make sense to try to
    include this method in BaseCluster.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def tear_down(self):
        """ Tear down the cluster.

        For ephemeral clusters, this should include destroying the cluster and
        freeing the associated resources.

        For long-lived clusters, this would mean returning the cluster to a
        state in which future tests will run successfully. Unfortunately, this
        means that the tear-down method of a long-lived cluster necessarily
        knows stuff about how tests mutate the cluster. Opportunity for
        improvement?
        """
        pass

    @abc.abstractmethod
    def all_hosts(self):
        """The difference between the all_hosts() method and
        all_internal_hosts() is that all_hosts() returns the unique, "outside
        facing" hostnames that docker uses. On the other hand
        all_internal_hosts() returns the more human readable host aliases for
        the containers used internally between containers. For example the
        unique master host will look something like
        'master-07d1774e-72d7-45da-bf84-081cfaa5da9a', whereas the internal
        master host will be 'master'.

        :return: List of all hosts with the random suffix.
        """
        pass

    @abc.abstractmethod
    def all_internal_hosts(self):
        """See the docstring for all_hosts() for an explanation of the
        differences between this and all_hosts().

        Returns a list of all hosts with the random suffix removed.
        """
        pass

    @abc.abstractmethod
    def get_ip_address_dict(self):
        """Returns a dict containing entries mapping both internal and external
        hostnames to the IP address of the node. I.e. the resulting dict will
        contain two entries per host with the same IP address as follows:
        'master-07d1774e-72d7-45da-bf84-081cfaa5da9a': '192.168.21.79'
        'master': '192.168.21.79'
        """
        pass

    @abc.abstractmethod
    def stop_host(self, host_name):
        """Stops a host. Paradoxically, start_host doesn't seem to be required
        for the product tests to run successfully."""
        pass

    @abc.abstractmethod
    def get_down_hostname(self, host_name):
        """This is part of the magic involved in stopping a host. If you're
        rolling a new implementation, you should dig more deeply into the
        existing implementations, figure out how it all works, and update this
        comment.
        """
        pass

    @abc.abstractmethod
    def postinstall(self, installer):
        """Some installers need the cluster to do some work after they're run
        so as to get some cluster-specific knowledge into the files created by
        the installer. In particular, clusters that support persisting the
        state of the hosts and bringing up a new cluster from that state may
        need to update host information on the new cluster.
        """
        pass

    @abc.abstractmethod
    def exec_cmd_on_host(self, host, cmd, user=None, raise_error=True,
                         tty=False, invoke_sudo=False):
        pass

    @abc.abstractmethod
    def run_script_on_host(self, script_contents, host, tty=True):
        """Create a script on the remote host with the given content and execute it.

        NOTE: if tty is set to True then the results of the execution on stdout will
        have ^M (carriage return) at the end of every line. If doing string
        comparison of the output, turn off tty.

        :param script_contents: a string with the script contents
        :param host: the host where to execute the script
        :param tty: whether to execute the script with tty enabled
        """
        pass

    @abc.abstractmethod
    def write_content_to_host(self, content, remote_path, host):
        pass

    @abc.abstractmethod
    def copy_to_host(self, source_path, host, **kwargs):
        pass

    @abc.abstractproperty
    def master(self):
        """               +++++ WARNING +++++
        When overriding this property make sure the child class uses the @property decorator.
        The declaration of the property in the child should look like this:

        @property
        def master(self):
            return self._master

        Returns the hostname of the master node of the cluster"""
        pass

    @abc.abstractproperty
    def user(self):
        """               +++++ WARNING +++++
        When overriding this property make sure the child class uses the @property decorator.
        The declaration of the property in the child should look like this:

        @property
        def user(self):
            return self._user

        Returns the user with which to execute commands on the cluster"""
        pass

    @abc.abstractproperty
    def rpm_cache_dir(self):
        """               +++++ WARNING +++++
        When overriding this property make sure the child class uses the @property decorator.
        The declaration of the property in the child should look like this:

        @property
        def rpm_cache_dir(self):
            return self._rpm_cache_dir

        Return directory where to cache the presto RPM. For DockerCluster this can be the
        mount directory but for ConfigurableCluster where uploading the RPM involves a large
        latency, the RPM cache has to be different so it doesn't get deleted before every test."""
        pass

    @abc.abstractproperty
    def mount_dir(self):
        """               +++++ WARNING +++++
        When overriding this property make sure the child class uses the @property decorator.
        The declaration of the property in the child should look like this:

        @property
        def mount_dir(self):
            return self._mount_dir

        Return the mount directory of the cluster. The mount directory is the place where files, scripts
        and other resources needed by a test are uploaded. The mount directory may or may not be
        ephemeral; see the implementation of the tear_down() method to confirm."""
        pass

    def ensure_correct_execution_environment(self):
        """Make sure the cluster environment we're executing on conforms to our
        expectations.

        For now just check that the cluster has a single JDK installed.

        :return: without error if only a single JDK is installed, otherwise exit
        """
        try:
            determine_jdk_directory(self)
        except Exception as e:
            sys.stderr.write(e.message)
            sys.stderr.flush()
            sys.exit(1)
