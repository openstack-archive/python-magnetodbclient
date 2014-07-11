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


class Client(client_base.ClientBase):
    """Client for the OpenStack MagnetoDB v1 API.

    :param string username: Username for authentication. (optional)
    :param string password: Password for authentication. (optional)
    :param string token: Token for authentication. (optional)
    :param string tenant_name: Tenant name. (optional)
    :param string tenant_id: Tenant id. (optional)
    :param string auth_url: Keystone service endpoint for authorization.
    :param string service_type: Keyvalue service type to pull from the
                                keystone catalog (e.g. 'keyvalue') (optional)
    :param string endpoint_type: Keyvalue service endpoint type to pull from
                                 the keystone catalog (e.g. 'publicURL',
                                 'internalURL', or 'adminURL') (optional)
    :param string region_name: Name of a region to select when choosing an
                               endpoint from the service catalog.
    :param string endpoint_url: A user-supplied endpoint URL for the magnetodb
                            service.  Lazy-authentication is possible for API
                            service calls if endpoint is set at
                            instantiation.(optional)
    :param integer timeout: Allows customization of the timeout for client
                            http requests. (optional)
    :param bool insecure: SSL certificate validation. (optional)
    :param string ca_cert: SSL CA bundle file to use. (optional)

    Example::

        from magnetodbclient.v1 import client
        magnetodb = client.Client(username=USER,
                                  password=PASS,
                                  tenant_name=TENANT_NAME,
                                  auth_url=KEYSTONE_URL)

        tables = magnetodb.list_tables()
        ...

    """

    base_path = "/data"
    tables_path = base_path + "/tables"
    table_path = base_path + "/tables/%s"
    put_item_path = table_path + "/put_item"
    update_item_path = table_path + "/update_item"
    delete_item_path = table_path + "/delete_item"
    get_item_path = table_path + "/get_item"
    query_path = table_path + "/query"
    scan_path = table_path + "/scan"
    batch_write_item_path = base_path + "/batch_write_item"
    batch_get_item_path = base_path + "/batch_get_item"

    def create_table(self, request_body):
        """Create table."""
        return self.post(self.tables_path, request_body)

    def delete_table(self, table_name):
        """Delete the specified table."""
        return self.delete(self.table_path % table_name)

    def list_tables(self, **params):
        """List tables."""
        return self.get(self.tables_path, params=params)

    def describe_table(self, table_name):
        """Describe the specified table."""
        return self.get(self.table_path % table_name)

    def put_item(self, table_name, request_body):
        """Put item to the specified table."""
        return self.post(self.put_item_path % table_name, request_body)

    def update_item(self, table_name, request_body):
        """Update item."""
        return self.post(self.update_item_path % table_name, request_body)

    def delete_item(self, table_name, request_body):
        """Delete item."""
        return self.post(self.delete_item_path % table_name, request_body)

    def get_item(self, table_name, request_body):
        """Get item."""
        return self.post(self.get_item_path % table_name, request_body)

    def query(self, table_name, request_body):
        """Query the specified table."""
        return self.post(self.query_path % table_name, request_body)

    def scan(self, table_name, request_body):
        """Scan the specified table."""
        return self.post(self.scan_path % table_name, request_body)

    def batch_write_item(self, request_items):
        """Batch write item."""
        return self.post(self.batch_write_item_path, request_items)

    def batch_get_item(self, request_items):
        """Batch get item."""
        return self.post(self.batch_get_item_path, request_items)
