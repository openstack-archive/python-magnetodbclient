# Copyright 2014 Symantec Corporation
# All Rights Reserved
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

import logging

from magnetodbclient.v1 import client_base


_logger = logging.getLogger(__name__)


class StreamingClient(client_base.ClientBase):
    """Client for the OpenStack MagnetoDB v1 streaming API.
    """
    base_path = "/data"
    table_path = base_path + "/tables/%s"
    bulk_load_path = table_path + "/bulk_load"

    def upload(self, table_name, body):
        """Bulk load."""
        return self.post(self.bulk_load_path % table_name, body)
