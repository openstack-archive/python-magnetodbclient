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


class ClientBase(object):

    # 8192 Is the default max URI len for eventlet.wsgi.server
    MAX_URI_LEN = 8192

    def __init__(self, **kwargs):
        """Initialize a new client for the MagnetoDB v1 API."""
        super(ClientBase, self).__init__()
        self.httpclient = client.HTTPClient(**kwargs)
        self.content_type = 'application/json'
        self.retries = 0
        self.retry_interval = 1

    def _handle_fault_response(self, status_code, response_body):
        # Create exception with HTTP status code and message
        _logger.debug(_("Error message: %s"), response_body)
        exception_handler_v1(status_code, response_body)

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

        if body and not isinstance(body, file):
            body = self.serialize(body)
        self.httpclient.content_type = self.content_type
        resp, replybody = self.httpclient.do_request(action, method, body=body)
        status_code = self.get_status_code(resp)
        if status_code in (httplib.OK,
                           httplib.CREATED,
                           httplib.ACCEPTED,
                           httplib.NO_CONTENT):
            return replybody
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
            return response.status_code

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
