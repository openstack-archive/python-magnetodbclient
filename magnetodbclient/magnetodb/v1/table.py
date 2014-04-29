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

import argparse
import logging

from cliff import show

from magnetodbclient.common import exceptions
from magnetodbclient.common import utils
from magnetodbclient.magnetodb import v1 as magnetodb
from magnetodbclient.openstack.common.gettextutils import _


class CreateTable(magnetodb.MagnetoDBCommand, show.ShowOne):
    """Create a table for a given tenant."""

    log = logging.getLogger(__name__ + '.CreateTable')
    resource = 'table'

    def add_known_arguments(self, parser):
        parser.add_argument(
            '--request-file', metavar='FILE', dest='request_file_name',
            help=_('File that contains table description to create'))

    def run(self, parsed_args):
        self.log.debug('run(%s)', parsed_args)
        magnetodb_client = self.get_client()
        body = utils.get_file_contents(parsed_args.request_file_name)
        magnetodb_client.create_table(body)
        print((_('Created a new %(resource)s: %(name)s')
               % {'name': body['table_name'],
                  'resource': self.resource}),
              file=self.app.stdout)
        return
#    def get_data(self, parsed_args):
#        self.log.debug('get_data(%s)' % parsed_args)
#        magnetodb_client = self.get_client()
#        body = utils.get_file_contents(parsed_args.request_file_name)
#        data = magnetodb_client.create_table(body)
#        info = {'Table Name': data['table_description']['table_name']}
#        print(_('Created a new %s:') % self.resource, file=self.app.stdout)
#        return zip(*sorted(info.iteritems()))


class DeleteTable(magnetodb.MagnetoDBCommand):
    """Delete a given table."""

    log = logging.getLogger(__name__ + '.DeleteTable')
    resource = 'table'

    def get_parser(self, prog_name):
        parser = super(DeleteTable, self).get_parser(prog_name)
        help_str = _('Name of %s to delete')
        parser.add_argument(
            'name', metavar=self.resource.upper(),
            help=help_str % self.resource)
        return parser

    def run(self, parsed_args):
        self.log.debug('run(%s)', parsed_args)
        magnetodb_client = self.get_client()
        magnetodb_client.delete_table(parsed_args.name)
        print((_('Deleted %(resource)s: %(name)s')
               % {'name': parsed_args.name,
                  'resource': self.resource}),
              file=self.app.stdout)
        return


class ListTable(magnetodb.ListCommand):
    """List tables that belong to a given tenant."""

    resource = 'table'
    log = logging.getLogger(__name__ + '.ListTable')
    list_columns = {'href': 'Table Name'}
    pagination_support = True
    sorting_support = True


class DescribeTable(magnetodb.DescribeCommand):
    """Show information of a given table."""

    resource = 'table'
    log = logging.getLogger(__name__ + '.DescribeTable')

    def format_output_data(self, data):
        try:
            del data[self.resource]['links']
        except Exception:
            pass
