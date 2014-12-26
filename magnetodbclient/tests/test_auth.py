# Copyright 2014 Symantec Corporation
# Copyright 2012 OpenStack Foundation
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

import copy
import uuid

import mock
from oslo_serialization import jsonutils
import requests
import testtools

from magnetodbclient import client
from magnetodbclient.common import exceptions


USERNAME = 'testuser'
USER_ID = 'testuser_id'
TENANT_NAME = 'testtenant'
TENANT_ID = 'testtenant_id'
PASSWORD = 'password'
AUTH_URL = 'https://127.0.0.1:5000/v2.0'
ENDPOINT_URL = 'localurl'
ENDPOINT_OVERRIDE = 'otherurl'
TOKEN = 'tokentoken'
REGION = 'RegionTest'
NOAUTH = 'noauth'

KS_TOKEN_RESULT = {
    'access': {
        'token': {'id': TOKEN,
                  'expires': '2012-08-11T07:49:01Z',
                  'tenant': {'id': str(uuid.uuid1())}},
        'user': {'id': str(uuid.uuid1())},
        'serviceCatalog': [
            {'endpoints_links': [],
             'endpoints': [{'adminURL': ENDPOINT_URL,
                            'internalURL': ENDPOINT_URL,
                            'publicURL': ENDPOINT_URL,
                            'region': REGION}],
             'type': 'kv-storage',
             'name': 'MagnetoDB'}
        ]
    }
}

ENDPOINTS_RESULT = {
    'endpoints': [{
        'type': 'kv-storage',
        'name': 'MagnetoDB',
        'region': REGION,
        'adminURL': ENDPOINT_URL,
        'internalURL': ENDPOINT_URL,
        'publicURL': ENDPOINT_URL
    }]
}


def get_response(status_code, headers=None):
    response = mock.Mock(return_value=requests.Response)
    response.headers = headers or {}
    response.status_code = status_code
    return response


class CLITestAuthNoAuth(testtools.TestCase):

    def setUp(self):
        """Prepare the test environment."""
        super(CLITestAuthNoAuth, self).setUp()
        self.client = client.HTTPClient(username=USERNAME,
                                        tenant_name=TENANT_NAME,
                                        password=PASSWORD,
                                        endpoint_url=ENDPOINT_URL,
                                        auth_strategy=NOAUTH,
                                        region_name=REGION)

    def test_get_noauth(self):
        with mock.patch.object(self.client, "request"):
            res200 = get_response(200)
            self.client.request.return_value = (res200, '')
            self.client.do_request('/resource', 'GET')
            self.client.request.assert_called_ones_with(
                ENDPOINT_URL + '/resource',
                'GET',
                headers={})
            self.assertEqual(self.client.endpoint_url, ENDPOINT_URL)


class CLITestAuthKeystone(testtools.TestCase):

    # Auth Body expected
    auth_body = {
        "auth": {
            "tenantName": "testtenant",
            "passwordCredentials": {
                "username": "testuser",
                "password": "password",
            }
        }
    }
    auth_body = jsonutils.dumps(auth_body)

    def setUp(self):
        """Prepare the test environment."""
        super(CLITestAuthKeystone, self).setUp()
        self.client = client.HTTPClient(username=USERNAME,
                                        tenant_name=TENANT_NAME,
                                        password=PASSWORD,
                                        auth_url=AUTH_URL,
                                        region_name=REGION)

    def test_reused_token_get_auth_info(self):
        """Test that Client.get_auth_info() works even if client was
           instantiated with predefined token.
        """

        client_ = client.HTTPClient(username=USERNAME,
                                    tenant_name=TENANT_NAME,
                                    token=TOKEN,
                                    password=PASSWORD,
                                    auth_url=AUTH_URL,
                                    region_name=REGION)
        expected = {'auth_token': TOKEN,
                    'auth_tenant_id': None,
                    'auth_user_id': None,
                    'endpoint_url': self.client.endpoint_url}
        self.assertEqual(client_.get_auth_info(), expected)

    def test_get_token(self):
        with mock.patch.object(self.client, "request"):
            res200 = get_response(200)
            self.client.request.return_value = (res200, KS_TOKEN_RESULT)
            self.client.do_request('/resource', 'GET')
            self.client.request.assert_called_ones_with(
                AUTH_URL + '/tokens', 'POST',
                body=self.auth_body, headers={})
            self.assertEqual(self.client.endpoint_url, ENDPOINT_URL)
            self.assertEqual(self.client.auth_token, TOKEN)

    def test_refresh_token(self):
        with mock.patch.object(self.client, "request"):
            self.client.auth_token = TOKEN
            self.client.endpoint_url = ENDPOINT_URL

            res200 = get_response(200)
            return_values = [exceptions.Unauthorized(),
                             (res200, KS_TOKEN_RESULT),
                             (res200, '')]

            def request_mock(*args, **kwargs):
                value = return_values.pop(0)
                if isinstance(value, Exception):
                    raise value
                else:
                    return value

            self.client.request.side_effect = request_mock

            self.client.do_request('/resource', 'GET')

            self.client.request.assert_has_calls([
                mock.call(
                    ENDPOINT_URL + '/resource', 'GET',
                    headers={'X-Auth-Token': TOKEN}),
                mock.call(
                    AUTH_URL + '/tokens', 'POST',
                    headers={},
                    body=self.auth_body),
                mock.call(
                    ENDPOINT_URL + '/resource', 'GET',
                    headers={'X-Auth-Token': TOKEN})
            ])

    def test_refresh_token_no_auth_url(self):
        with mock.patch.object(self.client, "request"):
            self.client.auth_url = None

            self.client.auth_token = TOKEN
            self.client.endpoint_url = ENDPOINT_URL

            self.client.request.side_effect = exceptions.Unauthorized

            self.assertRaises(exceptions.NoAuthURLProvided,
                              self.client.do_request,
                              '/resource',
                              'GET')

    def test_get_endpoint_url_with_invalid_auth_url(self):
        # Handle the case when auth_url is not provided
        self.client.auth_url = None
        self.assertRaises(exceptions.NoAuthURLProvided,
                          self.client._get_endpoint_url)

    def test_get_endpoint_url(self):
        with mock.patch.object(self.client, "request"):
            self.client.auth_token = TOKEN

            res200 = get_response(200)
            self.client.request.side_effect = [(res200, ENDPOINTS_RESULT),
                                               (res200, '')]
            self.client.do_request('/resource', 'GET')
            self.client.request.assert_has_calls([
                mock.call(AUTH_URL + '/tokens/%s/endpoints' % TOKEN, 'GET',
                          headers={'X-Auth-Token': TOKEN}),
                mock.call(ENDPOINT_URL + '/resource', 'GET',
                          headers={'X-Auth-Token': TOKEN})
            ])

    def test_use_given_endpoint_url(self):
        self.client = client.HTTPClient(
            username=USERNAME, tenant_name=TENANT_NAME, password=PASSWORD,
            auth_url=AUTH_URL, region_name=REGION,
            endpoint_url=ENDPOINT_OVERRIDE)
        self.assertEqual(self.client.endpoint_url, ENDPOINT_OVERRIDE)

        with mock.patch.object(self.client, "request"):
            self.client.auth_token = TOKEN
            res200 = get_response(200)
            self.client.request.return_value = (res200, '')
            self.client.do_request('/resource', 'GET')
            self.client.request.assert_called_ones_with(
                ENDPOINT_OVERRIDE + '/resource', 'GET',
                headers={'X-Auth-Token': TOKEN})
            self.assertEqual(self.client.endpoint_url, ENDPOINT_OVERRIDE)

    def test_get_endpoint_url_other(self):
        self.client = client.HTTPClient(
            username=USERNAME, tenant_name=TENANT_NAME, password=PASSWORD,
            auth_url=AUTH_URL, region_name=REGION, endpoint_type='otherURL')

        with mock.patch.object(self.client, "request"):
            self.client.auth_token = TOKEN
            res200 = get_response(200)
            self.client.request.return_value = (res200, ENDPOINTS_RESULT)
            self.assertRaises(exceptions.EndpointTypeNotFound,
                              self.client.do_request,
                              '/resource',
                              'GET')
            self.client.request.assert_called_ones_with(
                AUTH_URL + '/tokens/%s/endpoints' % TOKEN, 'GET',
                headers={'X-Auth-Token': TOKEN})

    def test_get_endpoint_url_failed(self):
        with mock.patch.object(self.client, "request"):
            self.client.auth_token = TOKEN
            res200 = get_response(200)

            return_values = [exceptions.Unauthorized(),
                             (res200, KS_TOKEN_RESULT),
                             (res200, ENDPOINTS_RESULT),
                             (res200, '')]

            def request_mock(*args, **kwargs):
                value = return_values.pop(0)
                if isinstance(value, Exception):
                    raise value
                else:
                    return value

            self.client.request.side_effect = request_mock
            self.client.do_request('/resource', 'GET')

            self.client.request.assert_has_calls([
                mock.call(AUTH_URL + '/tokens/%s/endpoints' % TOKEN, 'GET',
                          headers={'X-Auth-Token': TOKEN}),
                mock.call(AUTH_URL + '/tokens', 'POST',
                          body=self.auth_body, headers={}),
                mock.call(AUTH_URL + '/tokens/%s/endpoints' % TOKEN, 'GET',
                          headers={'X-Auth-Token': TOKEN}),
                mock.call(ENDPOINT_URL + '/resource', 'GET',
                          headers={'X-Auth-Token': TOKEN})
            ])

    def test_endpoint_type(self):
        resources = copy.deepcopy(KS_TOKEN_RESULT)
        endpoints = resources['access']['serviceCatalog'][0]['endpoints'][0]
        endpoints['internalURL'] = 'internal'
        endpoints['adminURL'] = 'admin'
        endpoints['publicURL'] = 'public'

        # Test default behavior is to choose public.
        self.client = client.HTTPClient(
            username=USERNAME, tenant_name=TENANT_NAME, password=PASSWORD,
            auth_url=AUTH_URL, region_name=REGION)

        self.client._extract_service_catalog(resources)
        self.assertEqual(self.client.endpoint_url, 'public')

        # Test admin url
        self.client = client.HTTPClient(
            username=USERNAME, tenant_name=TENANT_NAME, password=PASSWORD,
            auth_url=AUTH_URL, region_name=REGION, endpoint_type='adminURL')

        self.client._extract_service_catalog(resources)
        self.assertEqual(self.client.endpoint_url, 'admin')

        # Test public url
        self.client = client.HTTPClient(
            username=USERNAME, tenant_name=TENANT_NAME, password=PASSWORD,
            auth_url=AUTH_URL, region_name=REGION, endpoint_type='publicURL')

        self.client._extract_service_catalog(resources)
        self.assertEqual(self.client.endpoint_url, 'public')

        # Test internal url
        self.client = client.HTTPClient(
            username=USERNAME, tenant_name=TENANT_NAME, password=PASSWORD,
            auth_url=AUTH_URL, region_name=REGION, endpoint_type='internalURL')

        self.client._extract_service_catalog(resources)
        self.assertEqual(self.client.endpoint_url, 'internal')

        # Test url that isn't found in the service catalog
        self.client = client.HTTPClient(
            username=USERNAME, tenant_name=TENANT_NAME, password=PASSWORD,
            auth_url=AUTH_URL, region_name=REGION, endpoint_type='privateURL')

        self.assertRaises(exceptions.EndpointTypeNotFound,
                          self.client._extract_service_catalog,
                          resources)
