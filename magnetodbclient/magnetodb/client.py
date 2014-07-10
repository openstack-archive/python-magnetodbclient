# Copyright 2012 OpenStack Foundation.
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

from magnetodbclient.common import exceptions
from magnetodbclient.common import utils
from magnetodbclient.openstack.common.gettextutils import _


API_VERSIONS = {
    '1': 'magnetodbclient.v1.client.Client',
}

STREAMING_API_VERSIONS = {
    '1': 'magnetodbclient.v1.streaming_client.StreamingClient',
}

API = {
    'keyvalue': API_VERSIONS,
    'kv-streaming': STREAMING_API_VERSIONS,
}


def make_client(instance):
    """Returns an magnetodb client.
    """
    api_name = instance._api_name
    magnetodb_client = utils.get_client_class(
        instance._api_name,
        instance._api_version[api_name],
        API,
    )
    instance.initialize()
    url = instance._url
    url = url.rstrip("/")
    if '1' == instance._api_version[api_name]:
        client = magnetodb_client(username=instance._username,
                                  tenant_name=instance._tenant_name,
                                  password=instance._password,
                                  region_name=instance._region_name,
                                  auth_url=instance._auth_url,
                                  endpoint_url=url,
                                  token=instance._token,
                                  auth_strategy=instance._auth_strategy,
                                  insecure=instance._insecure,
                                  ca_cert=instance._ca_cert)
        return client
    else:
        raise exceptions.UnsupportedVersion(_("API version %s is not "
                                              "supported") %
                                            instance._api_version[api_name])
