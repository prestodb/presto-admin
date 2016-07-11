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
System tests for error handling in presto-admin
"""
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_PA_CLUSTER


class TestErrorHandling(BaseProductTestCase):

    def setUp(self):
        super(TestErrorHandling, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)
        self.upload_topology()

    def test_wrong_arguments_parallel(self):
        actual = self.run_prestoadmin('server start extra_arg',
                                      raise_error=False)
        expected = "Incorrect number of arguments to task.\n\n" \
                   "Displaying detailed information for task " \
                   "'server start':\n\n    Start the Presto server on all " \
                   "nodes\n    \n    A status check is performed on the " \
                   "entire cluster and a list of\n    servers that did not " \
                   "start, if any, are reported at the end.\n\n"
        self.assertEqual(expected, actual)

    def test_wrong_arguments_serial(self):
        actual = self.run_prestoadmin('server start extra_arg --serial',
                                      raise_error=False)
        expected = "Incorrect number of arguments to task.\n\n" \
                   "Displaying detailed information for task " \
                   "'server start':\n\n    Start the Presto server on all " \
                   "nodes\n    \n    A status check is performed on the " \
                   "entire cluster and a list of\n    servers that did not " \
                   "start, if any, are reported at the end.\n\n"
        self.assertEqual(expected, actual)
