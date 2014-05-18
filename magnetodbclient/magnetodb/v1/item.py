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

from __future__ import print_function

import logging

from magnetodbclient.common import exceptions
from magnetodbclient.common import utils
from magnetodbclient.magnetodb import v1 as magnetodbv1
from magnetodbclient.openstack.common.gettextutils import _


class GetItem(magnetodbv1.ShowCommand):
    """Gets item from a given table by key."""

    resource = 'item'
    resource_path = ('item',)
    method = 'get_item'
    required_args = ('request_file',)
    log = logging.getLogger(__name__ + '.GetItem')

    def add_known_arguments(self, parser):
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=_('Name of table to look up'))
        parser.add_argument(
            '--request-file', metavar='FILE',
            help=_('File that contains item description to put in table'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file)

    def call_server(self, magnetodb_client, name, parsed_args, body):
        obj_shower = getattr(magnetodb_client, self.method)
        data = obj_shower(name, body)
        return data


class PutItem(magnetodbv1.CreateCommand):
    """Puts item to a given table."""

    resource = 'item'
    resource_path = ('attributes',)
    method = 'put_item'
    required_args = ('request_file',)
    log = logging.getLogger(__name__ + '.PutItem')

    def add_known_arguments(self, parser):
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=_('Name of table to put item in'))
        parser.add_argument(
            '--request-file', metavar='FILE',
            help=_('File that contains item description to put in table'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file)


class DeleteItem(magnetodbv1.CreateCommand):
    """Deletes item from a given table."""

    resource = 'item'
    method = 'delete_item'
    resource_path = ('attributes',)
    required_args = ('request_file',)
    success_message = _('Deleted %s:')
    log = logging.getLogger(__name__ + '.DeleteItem')

    def add_known_arguments(self, parser):
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=_('Name of table to delete item from'))
        parser.add_argument(
            '--request-file', metavar='FILE', dest='request_file',
            help=_('File that contains item key description'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file)


class UpdateItem(magnetodbv1.UpdateCommand):
    """Updates item in a given table."""

    log = logging.getLogger(__name__ + '.UpdateItem')
    method = 'update_item'
    resource_path = ('attributes',)
    resource = 'item'

    # NOTE(aostapenko) Update item is not supported on server side yet
    # remove this method in future
    def run(self, parsed_args):
        print("Update item is not supported now on server side")

    def add_known_arguments(self, parser):
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=_('Name of table to update item'))
        parser.add_argument(
            '--request-file', metavar='FILE',
            help=_('File that contains item update description'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file)


class Query(magnetodbv1.ListCommand):
    """Query table that belong to a given tenant."""

    resource = 'item'
    resource_path = ('items',)
    method = 'query'
    required_args = ('request_file',)
    log = logging.getLogger(__name__ + '.Query')

    def add_known_arguments(self, parser):
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=_('Name of table to query'))
        parser.add_argument(
            '--request-file', metavar='FILE', dest='request_file',
            help=_('File that contains query request description'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file)

    def call_server(self, magnetodb_client, search_opts, parsed_args, body):
        obj_lister = getattr(magnetodb_client, self.method)
        data = obj_lister(parsed_args.name, body, **search_opts)
        return data


class Scan(Query):
    """Scan table that belong to a given tenant."""

    method = 'scan'
    log = logging.getLogger(__name__ + '.Scan')


class BatchWrite(magnetodbv1.ListCommand):
    """Batch write command."""

    resource_path = ('unprocessed_items',)
    method = 'batch_write_item'
    required_args = ('request_file',)
    log = logging.getLogger(__name__ + '.GetItem')
    success_message = _("Unprocessed items:")
    list_columns = ['Table Name', 'Request Type', 'Request']

    def add_known_arguments(self, parser):
        parser.add_argument(
            '--request-file', metavar='FILE',
            help=_('File that contains item description to put in table'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file)

    def call_server(self, magnetodb_client, name, parsed_args, body):
        obj_shower = getattr(magnetodb_client, self.method)
        data = obj_shower(body)
        return data

    def _get_info(self, data, parsed_args):
        data = super(BatchWrite, self)._get_info(data, parsed_args)
        if not data:
            return data
        output_list = []
        for table_name, requests in data.iteritems():
            for request in requests:
                for request_type, request_body in request.iteritems():
                    output_list.append({"table_name": table_name,
                                        "request": request_body,
                                        "request_type": request_type})
        return output_list
