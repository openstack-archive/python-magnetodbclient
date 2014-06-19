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

import httplib
import logging
import time
import urllib

import six.moves.urllib.parse as urlparse

from magnetodbclient import client
from magnetodbclient.common import _
from magnetodbclient.common import exceptions
from magnetodbclient.common import serializer
from magnetodbclient.common import utils


_logger = logging.getLogger(__name__)


def exception_handler_v1(status_code, error_content):
    """Exception handler for API v1 client

        This routine generates the appropriate
        MagnetoDB exception according to the contents of the
        response body

        :param status_code: HTTP error status code
        :param error_content: deserialized body of error response
    """
    error_dict = None
    if isinstance(error_content, dict):
        error_dict = error_content.get('MagnetoDBError')
    # Find real error type
    bad_magnetodb_error_flag = False
    if error_dict:
        # If MagnetoDB key is found, it will definitely contain
        # a 'message' and 'type' keys?
        try:
            error_type = error_dict['type']
            error_message = error_dict['message']
            if error_dict['detail']:
                error_message += "\n" + error_dict['detail']
        except Exception:
            bad_magnetodb_error_flag = True
        if not bad_magnetodb_error_flag:
            # If corresponding exception is defined, use it.
            client_exc = getattr(exceptions, '%sClient' % error_type, None)
            # Otherwise look up per status-code client exception
            if not client_exc:
                client_exc = exceptions.HTTP_EXCEPTION_MAP.get(status_code)
            if client_exc:
                raise client_exc(message=error_message,
                                 status_code=status_code)
            else:
                raise exceptions.MagnetoDBClientException(
                    status_code=status_code, message=error_message)
        else:
            raise exceptions.MagnetoDBClientException(status_code=status_code,
                                                      message=error_dict)
    else:
        message = None
        if isinstance(error_content, dict):
            message = error_content.get('message')
        if message:
            raise exceptions.MagnetoDBClientException(status_code=status_code,
                                                      message=message)

    # If we end up here the exception was not a magnetodb error
    msg = "%s-%s" % (status_code, error_content)
    raise exceptions.MagnetoDBClientException(status_code=status_code,
                                              message=msg)


class Client(object):
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

    # 8192 Is the default max URI len for eventlet.wsgi.server
    MAX_URI_LEN = 8192

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

    def __init__(self, **kwargs):
        """Initialize a new client for the MagnetoDB v1 API."""
        super(Client, self).__init__()
        self.httpclient = client.HTTPClient(**kwargs)
        self.content_type = 'application/json'
        self.retries = 0
        self.retry_interval = 1

    def _handle_fault_response(self, status_code, response_body):
        # Create exception with HTTP status code and message
        _logger.debug(_("Error message: %s"), response_body)
        # Add deserialized error message to exception arguments
        try:
            des_error_body = self.deserialize(response_body, status_code)
        except Exception:
            # If unable to deserialized body it is probably not a
            # MagnetoDB error
            des_error_body = {'message': response_body}
        # Raise the appropriate exception
        exception_handler_v1(status_code, des_error_body)

    def _check_uri_length(self, action):
        uri_len = len(self.httpclient.endpoint_url) + len(action)
        if uri_len > self.MAX_URI_LEN:
            raise exceptions.RequestURITooLong(
                excess=uri_len - self.MAX_URI_LEN)

    def do_request(self, method, action, body=None, headers=None, params=None):
        if type(params) is dict and params:
            params = utils.safe_encode_dict(params)
            action += '?' + urllib.urlencode(params, doseq=1)
        # Ensure client always has correct uri - do not guesstimate anything
        self.httpclient.authenticate_and_fetch_endpoint_url()
        self._check_uri_length(action)

        if body:
            body = self.serialize(body)
        self.httpclient.content_type = self.content_type
        resp, replybody = self.httpclient.do_request(action, method, body=body)
        status_code = self.get_status_code(resp)
        if status_code in (httplib.OK,
                           httplib.CREATED,
                           httplib.ACCEPTED,
                           httplib.NO_CONTENT):
            return self.deserialize(replybody, status_code)
        else:
            if not replybody:
                replybody = resp.reason
            self._handle_fault_response(status_code, replybody)

    def get_auth_info(self):
        return self.httpclient.get_auth_info()

    def get_status_code(self, response):
        """Returns the integer status code from the response.

        Either a Webob.Response (used in testing) or httplib.Response
        is returned.
        """
        if hasattr(response, 'status_int'):
            return response.status_int
        else:
            return response.status

    def serialize(self, data):
        """Serializes a dictionary into either xml or json.

        A dictionary with a single key can be passed and
        it can contain any structure.
        """
        if data is None:
            return None
        elif type(data) is dict:
            return serializer.Serializer().serialize(data, self.content_type)
        else:
            raise Exception(_("Unable to serialize object of type = '%s'") %
                            type(data))

    def deserialize(self, data, status_code):
        """Deserializes an xml or json string into a dictionary."""
        if status_code == 204:
            return data
        return serializer.Serializer().deserialize(
            data, self.content_type)['body']

    def retry_request(self, method, action, body=None,
                      headers=None, params=None):
        """Call do_request with the default retry configuration.

        Only idempotent requests should retry failed connection attempts.
        :raises: ConnectionFailed if the maximum # of retries is exceeded
        """
        max_attempts = self.retries + 1
        for i in range(max_attempts):
            try:
                return self.do_request(method, action, body=body,
                                       headers=headers, params=params)
            except exceptions.ConnectionFailed:
                # Exception has already been logged by do_request()
                if i < self.retries:
                    _logger.debug(_('Retrying connection '
                                    'to MagnetoDB service'))
                    time.sleep(self.retry_interval)

        raise exceptions.ConnectionFailed(reason=_("Maximum attempts reached"))

    def delete(self, action, body=None, headers=None, params=None):
        return self.retry_request("DELETE", action, body=body,
                                  headers=headers, params=params)

    def get(self, action, body=None, headers=None, params=None):
        return self.retry_request("GET", action, body=body,
                                  headers=headers, params=params)

    def post(self, action, body=None, headers=None, params=None):
        # Do not retry POST requests to avoid the orphan objects problem.
        return self.do_request("POST", action, body=body,
                               headers=headers, params=params)

    def put(self, action, body=None, headers=None, params=None):
        return self.retry_request("PUT", action, body=body,
                                  headers=headers, params=params)

    def list(self, collection, path, retrieve_all=True, **params):
        if retrieve_all:
            res = []
            for r in self._pagination(collection, path, **params):
                res.extend(r[collection])
            return {collection: res}
        else:
            return self._pagination(collection, path, **params)

    def _pagination(self, collection, path, **params):
        if params.get('page_reverse', False):
            linkrel = 'previous'
        else:
            linkrel = 'next'
        next = True
        while next:
            res = self.get(path, params=params)
            yield res
            next = False
            try:
                for link in res['%s_links' % collection]:
                    if link['rel'] == linkrel:
                        query_str = urlparse.urlparse(link['href']).query
                        params = urlparse.parse_qs(query_str)
                        next = True
                        break
            except KeyError:
                break
