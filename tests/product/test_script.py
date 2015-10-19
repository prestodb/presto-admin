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
Test script run
"""
from nose.plugins.attrib import attr

from tests.product.base_product_case import BaseProductTestCase


class TestScript(BaseProductTestCase):
    def setUp(self):
        super(TestScript, self).setUp()
        self.setup_cluster('pa_only')
        self.upload_topology()

    @attr('smoketest')
    def test_run_script(self):
        # basic run script
        self.cluster.write_content_to_host('#!/bin/bash\necho hello',
                                           '/opt/prestoadmin/script.sh',
                                           self.cluster.master)
        output = self.run_prestoadmin('script run /opt/prestoadmin/script.sh')
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
                                           '/opt/prestoadmin/script.sh',
                                           self.cluster.master)
        output = self.run_prestoadmin('script run /opt/prestoadmin/script.sh',
                                      '/opt/script.sh')
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
        output = self.run_prestoadmin('script run /opt/prestoadmin/script.sh')
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
                                           '/opt/prestoadmin/invalid.sh',
                                           self.cluster.master)
        output = self.run_prestoadmin('script run /opt/prestoadmin/invalid.sh',
                                      raise_error=False)
        self.assertEqualIgnoringOrder(output, """
Fatal error: [slave2] sudo() received nonzero return code 127 while executing!

Requested: /tmp/invalid.sh
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "/tmp/invalid.sh"

Aborting.
[slave2] out: /tmp/invalid.sh: line 1: not: command not found
[slave2] out:

Fatal error: [master] sudo() received nonzero return code 127 while executing!

Requested: /tmp/invalid.sh
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "/tmp/invalid.sh"

Aborting.
[master] out: /tmp/invalid.sh: line 1: not: command not found
[master] out:

Fatal error: [slave3] sudo() received nonzero return code 127 while executing!

Requested: /tmp/invalid.sh
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "/tmp/invalid.sh"

Aborting.
[slave3] out: /tmp/invalid.sh: line 1: not: command not found
[slave3] out:

Fatal error: [slave1] sudo() received nonzero return code 127 while executing!

Requested: /tmp/invalid.sh
Executed: sudo -S -p 'sudo password:'  /bin/bash -l -c "/tmp/invalid.sh"

Aborting.
[slave1] out: /tmp/invalid.sh: line 1: not: command not found
[slave1] out:
""")
