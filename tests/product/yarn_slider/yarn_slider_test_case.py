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

from errno import ECONNREFUSED
from socket import socket, error
import time

from tests.product.base_product_case import BaseProductTestCase

HDFS_PORT = 8020


class YarnSliderTestCase(BaseProductTestCase):
    def setUp(self):
        super(YarnSliderTestCase, self).setUp()

    def await_hdfs(self):
        start = time.clock()
        ip_addr = self.cluster.get_ip_address_dict()[self.cluster.master]
        while True:
            try:
                s = socket()
                s.connect((ip_addr, HDFS_PORT))
                break
            except error as e:
                s.close()
                if e.errno == ECONNREFUSED:
                    pass
                else:
                    raise
            finally:
                s.close()
        end = time.clock()
        duration = end - start
        print 'Waited %.3f seconds for hdfs' % (duration)
