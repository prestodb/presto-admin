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

import os

from nose.plugins.attrib import attr

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_PA_CLUSTER
from tests.product.config_dir_utils import get_config_file_path
from tests.product.constants import LOCAL_RESOURCES_DIR


topology_with_slave1_coord = """{{'coordinator': u'slave1',
 'port': 22,
 'username': '{user}',
 'workers': [u'master',
             u'slave2',
             u'slave3']}}
"""

normal_topology = """{{'coordinator': u'master',
 'port': 22,
 'username': '{user}',
 'workers': [u'slave1',
             u'slave2',
             u'slave3']}}
"""

local_topology = """{{'coordinator': 'localhost',
 'port': 22,
 'username': '{user}',
 'workers': ['localhost']}}
"""


class TestTopologyShow(BaseProductTestCase):

    def setUp(self):
        super(TestTopologyShow, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)

    @attr('smoketest')
    def test_topology_show(self):
        self.upload_topology()
        actual = self.run_prestoadmin('topology show')
        expected = normal_topology.format(user=self.cluster.user)
        self.assertEqual(expected, actual)

    def test_topology_show_empty_config(self):
        self.dump_and_cp_topology(topology={})
        actual = self.run_prestoadmin('topology show')
        self.assertEqual(local_topology.format(user=self.cluster.user), actual)

    def test_topology_show_bad_json(self):
        self.cluster.copy_to_host(
            os.path.join(LOCAL_RESOURCES_DIR, 'invalid_json.json'),
            self.cluster.master
        )
        self.cluster.exec_cmd_on_host(
            self.cluster.master,
            'cp %s %s' %
            (os.path.join(self.cluster.mount_dir, 'invalid_json.json'), get_config_file_path())
        )
        self.assertRaisesRegexp(OSError,
                                'Expecting , delimiter: line 3 column 3 '
                                '\(char 21\)  More detailed information '
                                'can be found in '
                                '.*/.prestoadmin/log/presto-admin.log\n',
                                self.run_prestoadmin,
                                'topology show')
