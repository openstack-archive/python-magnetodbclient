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

try:
    import json
except ImportError:
    import simplejson as json
import logging
import os
import requests
import time


from magnetodbclient.common import exceptions
from magnetodbclient.common import utils
from magnetodbclient.openstack.common.gettextutils import _

_logger = logging.getLogger(__name__)
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

if os.environ.get('MAGNETODBCLIENT_DEBUG'):
    ch = logging.StreamHandler()
    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(ch)


class ServiceCatalog(object):
    """Helper methods for dealing with a Keystone Service Catalog."""

    def __init__(self, resource_dict):
        self.catalog = resource_dict

    def get_token(self):
        """Fetch token details from service catalog."""
        token = {'id': self.catalog['access']['token']['id'],
                 'expires': self.catalog['access']['token']['expires'], }
        try:
            token['user_id'] = self.catalog['access']['user']['id']
            token['tenant_id'] = (
                self.catalog['access']['token']['tenant']['id'])
        except Exception:
            # just leave the tenant and user out if it doesn't exist
            pass
        return token

    def url_for(self, attr=None, filter_value=None,
                service_type='kv-storage', endpoint_type='publicURL'):
        """Fetch the URL from the MagnetoDB service for
        a particular endpoint type. If none given, return
        publicURL.
        """

        catalog = self.catalog['access'].get('serviceCatalog', [])
        matching_endpoints = []
        for service in catalog:
            if service['type'] != service_type:
                continue

            endpoints = service['endpoints']
            for endpoint in endpoints:
                if not filter_value or endpoint.get(attr) == filter_value:
                    matching_endpoints.append(endpoint)

        if not matching_endpoints:
            raise exceptions.EndpointNotFound()
        elif len(matching_endpoints) > 1:
            raise exceptions.AmbiguousEndpoints(
                matching_endpoints=matching_endpoints)
        else:
            if endpoint_type not in matching_endpoints[0]:
                raise exceptions.EndpointTypeNotFound(type_=endpoint_type)

            return matching_endpoints[0][endpoint_type]


class ServiceCatalogV3(object):

    def __init__(self, resp, resource_dict):
        self.headers = resp.headers
        self.catalog = resource_dict

    def get_token(self):
        return self.headers['x-subject-token']

    def url_for(self, attr=None, filter_value=None,
                service_type='kv-storage', endpoint_type='public'):
        """Fetch the URL from the MagnetoDB service for
        a particular endpoint interface. If none given, return
        publicURL.
        """

        catalog = self.catalog['token'].get('catalog', [])
        matching_endpoints = []
        interface = endpoint_type[:-3]
        for service in catalog:
            if service['type'] != service_type:
                continue

            endpoints = service['endpoints']
            for endpoint in endpoints:
                if endpoint['interface'] == interface:
                    if not filter_value or endpoint.get(attr) == filter_value:
                        matching_endpoints.append(endpoint)

        if not matching_endpoints:
            raise exceptions.EndpointNotFound()
        elif len(matching_endpoints) > 1:
            raise exceptions.AmbiguousEndpoints(
                matching_endpoints=matching_endpoints)
        return matching_endpoints[0]['url']


class HTTPClient(object):
    """Handles the REST calls and responses, include authn."""

    USER_AGENT = 'python-magnetodbclient'

    def __init__(self, username=None, tenant_name=None, tenant_id=None,
                 password=None, auth_url=None, token=None, region_name=None,
                 timeout=None, endpoint_url=None, insecure=False,
                 endpoint_type='publicURL', auth_strategy='keystone',
                 ca_cert=None, log_credentials=False,
                 service_type='kv-storage', domain_id='default',
                 domain_name='Default',
                 **kwargs):

        self.username = username
        self.tenant_name = tenant_name
        self.tenant_id = tenant_id
        self.domain_name = domain_name
        self.domain_id = domain_id or 'default'
        self.password = password
        self.auth_url = auth_url.rstrip('/') if auth_url else None
        self.service_type = service_type
        self.endpoint_type = endpoint_type
        self.region_name = region_name
        self.auth_token = token
        self.auth_tenant_id = None
        self.auth_user_id = None
        self.content_type = 'application/json'
        self.endpoint_url = endpoint_url
        self.auth_strategy = auth_strategy
        self.log_credentials = log_credentials
        self.project_name = self.tenant_name
        self.project_id = self.tenant_id

        self.times = []

        if insecure:
            self.verify_cert = False
        else:
            self.verify_cert = ca_cert if ca_cert else True

    def request(self, url, method, **kwargs):
        kwargs.setdefault('headers', kwargs.get('headers', {}))
        kwargs['headers']['User-Agent'] = self.USER_AGENT
        kwargs['headers']['Accept'] = 'application/json'

        utils.http_log_req(_logger, (url, method), kwargs)

        if 'body' in kwargs:
            kwargs['headers']['Content-Type'] = 'application/json'
            kwargs['data'] = kwargs['body']
            del kwargs['body']

        resp = requests.request(
            method,
            url,
            verify=self.verify_cert,
            **kwargs)
        utils.http_log_resp(_logger, resp)

        if resp.text:
            if resp.status_code == 400:
                if ('Connection refused' in resp.text or
                    'actively refused' in resp.text):
                    raise exceptions.ConnectionRefused(resp.text)
            try:
                body = json.loads(resp.text)
            except ValueError:
                body = None
        else:
            body = None

        if resp.status_code >= 400:
            raise exceptions.from_response(resp, body, url)

        return resp, body

    def _time_request(self, url, method, **kwargs):
        start_time = time.time()
        resp, body = self.request(url, method, **kwargs)
        self.times.append(("%s %s" % (method, url),
                           start_time, time.time()))
        return resp, body

    def _cs_request(self, url, method, **kwargs):
        # Perform the request once. If we get a 401 back then it
        # might be because the auth token expired, so try to
        # re-authenticate and try again. If it still fails, bail.
        try:
            kwargs.setdefault('headers', {})['X-Auth-Token'] = self.auth_token
            if self.project_id:
                kwargs['headers']['X-Auth-Project-Id'] = self.project_id

            resp, body = self._time_request(url, method, **kwargs)
            return resp, body
        except exceptions.Unauthorized as ex:
            try:
                self.authenticate()
                kwargs['headers']['X-Auth-Token'] = self.auth_token
                resp, body = self._time_request(url, method, **kwargs)
                return resp, body
            except exceptions.Unauthorized:
                raise ex

    def _strip_credentials(self, kwargs):
        if kwargs.get('body') and self.password:
            log_kwargs = kwargs.copy()
            log_kwargs['body'] = kwargs['body'].replace(self.password,
                                                        'REDACTED')
            return log_kwargs
        else:
            return kwargs

    def authenticate_and_fetch_endpoint_url(self):
        if not self.auth_token:
            self.authenticate()
        elif not self.endpoint_url:
            self.endpoint_url = self._get_endpoint_url()

    def do_request(self, url, method, **kwargs):
        self.authenticate_and_fetch_endpoint_url()
        # Perform the request once. If we get a 401 back then it
        # might be because the auth token expired, so try to
        # re-authenticate and try again. If it still fails, bail.
        try:
            kwargs.setdefault('headers', {})
            if self.auth_token is None:
                self.auth_token = ""
            if self.auth_token:
                kwargs['headers']['X-Auth-Token'] = self.auth_token
            resp, body = self._cs_request(self.endpoint_url + url, method,
                                          **kwargs)
            return resp, body
        except exceptions.Unauthorized:
            self.authenticate()
            kwargs.setdefault('headers', {})
            kwargs['headers']['X-Auth-Token'] = self.auth_token
            resp, body = self._cs_request(
                self.endpoint_url + url, method, **kwargs)
            return resp, body

    def _extract_service_catalog(self, body):
        """Set the client's service catalog from the response data."""
        self.service_catalog = ServiceCatalog(body)
        try:
            sc = self.service_catalog.get_token()
            self.auth_token = sc['id']
            self.auth_tenant_id = sc.get('tenant_id')
            self.auth_user_id = sc.get('user_id')
        except KeyError:
            raise exceptions.Unauthorized()
        if not self.endpoint_url:
            self.endpoint_url = self.service_catalog.url_for(
                attr='region', filter_value=self.region_name,
                service_type=self.service_type,
                endpoint_type=self.endpoint_type)

    def _extract_service_catalog_v3(self, resp, body):
        """Set the client's service catalog from the response keystone v3 data.
        """
        self.service_catalog = ServiceCatalogV3(resp, body)
        try:
            self.auth_token = self.service_catalog.get_token()
        except KeyError:
            raise exceptions.Unauthorized()
        if not self.endpoint_url:
            self.endpoint_url = self.service_catalog.url_for(
                attr='region', filter_value=self.region_name,
                service_type=self.service_type,
                endpoint_type=self.endpoint_type)

    def _authenticate_keystone_v3(self):
        """Provides authentication using Identity API v3."""

        req_url = self.auth_url.rstrip('/') + '/auth/tokens'

        creds = {
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "domain": {"id": self.domain_id},
                            "name": self.username,
                            "password": self.password,
                        }
                    }
                },
                "scope": {
                    "project": {
                        "domain": {"id": self.domain_id},
                        "name": self.project_name
                    }
                }
            }
        }

        headers = {'Content-Type': 'application/json'}
        body = json.dumps(creds)
        resp, body = self._time_request(req_url, 'POST',
                                        headers=headers, body=body)

        self._extract_service_catalog_v3(resp, body)

    def _authenticate_keystone(self):
        if self.tenant_id:
            body = {'auth': {'passwordCredentials':
                             {'username': self.username,
                              'password': self.password, },
                             'tenantId': self.tenant_id, }, }
        else:
            body = {'auth': {'passwordCredentials':
                             {'username': self.username,
                              'password': self.password, },
                             'tenantName': self.tenant_name, }, }

        if self.auth_url is None:
            raise exceptions.NoAuthURLProvided()

        token_url = self.auth_url + "/tokens"

        # Make sure we follow redirects when trying to reach Keystone
        resp, resp_body = self._time_request(token_url, "POST",
                                             headers={},
                                             body=json.dumps(body))
        status_code = self.get_status_code(resp)
        if status_code != 200:
            raise exceptions.Unauthorized(message=resp_body)
        self._extract_service_catalog(resp_body)

    def _authenticate_noauth(self):
        if not self.endpoint_url:
            message = _('For "noauth" authentication strategy, the endpoint '
                        'must be specified either in the constructor or '
                        'using --os-url')
            raise exceptions.Unauthorized(message=message)

    def authenticate(self):
        if self.auth_strategy == 'keystone':
            if not self.auth_url:
                raise exceptions.NoAuthURLProvided()
            auth_api = self.auth_url.split('/')[-1]
            if auth_api == 'v2.0':
                self._authenticate_keystone()
            elif auth_api == 'v3':
                self._authenticate_keystone_v3()
            else:
                err_msg = _('Unknown Keystone api version: %s') % auth_api
                raise exceptions.Unauthorized(message=err_msg)
        elif self.auth_strategy == 'noauth':
            self._authenticate_noauth()
        else:
            err_msg = _('Unknown auth strategy: %s') % self.auth_strategy
            raise exceptions.Unauthorized(message=err_msg)

    def _get_endpoint_url(self):
        if self.auth_url is None:
            raise exceptions.NoAuthURLProvided()

        url = self.auth_url + '/tokens/%s/endpoints' % self.auth_token
        try:
            resp, body = self._cs_request(url, "GET")
        except exceptions.Unauthorized:
            self.authenticate()
            return self.endpoint_url

        for endpoint in body.get('endpoints', []):
            if (endpoint['type'] == 'kv-storage' and
                endpoint.get('region') == self.region_name):
                if self.endpoint_type not in endpoint:
                    raise exceptions.EndpointTypeNotFound(
                        type_=self.endpoint_type)
                return endpoint[self.endpoint_type]

        raise exceptions.EndpointNotFound()

    def get_auth_info(self):
        return {'auth_token': self.auth_token,
                'auth_tenant_id': self.auth_tenant_id,
                'auth_user_id': self.auth_user_id,
                'endpoint_url': self.endpoint_url}

    def get_status_code(self, response):
        """Returns the integer status code from the response."""
        return response.status_code
