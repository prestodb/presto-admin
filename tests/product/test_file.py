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
Test file run
"""
import os

from nose.plugins.attrib import attr

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_PA_CLUSTER
from tests.product.config_dir_utils import get_install_directory


class TestFile(BaseProductTestCase):
    def setUp(self):
        super(TestFile, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)
        self.upload_topology()

    @attr('smoketest')
    def test_run_script(self):
        script_path = os.path.join(get_install_directory(), 'script.sh')
        # basic run script
        self.cluster.write_content_to_host('#!/bin/bash\necho hello',
                                           script_path,
                                           self.cluster.master)
        output = self.run_prestoadmin('file run %s' % script_path)
        self.assertEqualIgnoringOrder(output, """[slave2] out: hello
[slave2] out:
[slave1] out: hello
[slave1] out:
[master] out: hello
[master] out:
[slave3] out: hello
[slave3] out:
""")
        # specify remote directory
        self.cluster.write_content_to_host('#!/bin/bash\necho hello',
                                           script_path,
                                           self.cluster.master)
        output = self.run_prestoadmin('file run %s' % script_path)
        self.assertEqualIgnoringOrder(output, """[slave2] out: hello
[slave2] out:
[slave1] out: hello
[slave1] out:
[master] out: hello
[master] out:
[slave3] out: hello
[slave3] out:
""")

        # remote and local are the same
        self.cluster.write_content_to_host('#!/bin/bash\necho hello',
                                           '/tmp/script.sh',
                                           self.cluster.master)
        output = self.run_prestoadmin('file run %s' % script_path)
        self.assertEqualIgnoringOrder(output, """[slave2] out: hello
[slave2] out:
[slave1] out: hello
[slave1] out:
[master] out: hello
[master] out:
[slave3] out: hello
[slave3] out:
""")
        # invalid script
        self.cluster.write_content_to_host('not a valid script',
                                           script_path,
                                           self.cluster.master)
        output = self.run_prestoadmin('file run %s' % script_path,
                                      raise_error=False)
        self.assertEqualIgnoringOrder(output, """
Fatal error: [slave2] sudo() received nonzero return code 127 while executing!

Requested: /tmp/script.sh
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "/tmp/script.sh"

Aborting.
[slave2] out: /tmp/script.sh: line 1: not: command not found
[slave2] out:

Fatal error: [master] sudo() received nonzero return code 127 while executing!

Requested: /tmp/script.sh
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "/tmp/script.sh"

Aborting.
[master] out: /tmp/script.sh: line 1: not: command not found
[master] out:

Fatal error: [slave3] sudo() received nonzero return code 127 while executing!

Requested: /tmp/script.sh
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "/tmp/script.sh"

Aborting.
[slave3] out: /tmp/script.sh: line 1: not: command not found
[slave3] out:

Fatal error: [slave1] sudo() received nonzero return code 127 while executing!

Requested: /tmp/script.sh
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "/tmp/script.sh"

Aborting.
[slave1] out: /tmp/script.sh: line 1: not: command not found
[slave1] out:
""")
