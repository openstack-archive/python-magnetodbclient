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
from magnetodbclient.magnetodb import v1 as magnetodbv1
from magnetodbclient.openstack.common.gettextutils import _


def _format_table_name(table):
    try:
        return table['href']
    except Exception:
        return ''


def _get_lsi_names(indexes):
    index_names = []
    for index in indexes:
        index_names.append(index['index_name'])
    return index_names


class ListTable(magnetodbv1.ListCommand):
    """List tables that belong to a given tenant."""

    resource_path = ('table',)
    method = 'list_tables'
    log = logging.getLogger(__name__ + '.ListTable')
    _formatters = {'Table Name': _format_table_name}
    list_columns = ['Table Name']


class ShowTable(magnetodbv1.ShowCommand):
    """Show information of a given table."""

    resource_path = ('table',)
    method = 'describe_table'
    excluded_rows = ('links',)
    _formatters = {'local_secondary_indexes': _get_lsi_names}
    log = logging.getLogger(__name__ + '.ShowTable')

    def add_known_arguments(self, parser):
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=_('Name of table to look up'))


class ListIndex(ShowTable):
    """List indices of a given table."""

    log = logging.getLogger(__name__ + '.ListIndex')

    def take_action(self, parsed_args):
        parsed_args.columns = ['local_secondary_indexes']
        data = self.get_data(parsed_args)
        return data


class ShowIndex(ShowTable):
    """Show information of a given index."""

    resource_path = ('table', 'local_secondary_indexes')
    log = logging.getLogger(__name__ + '.ShowIndex')

    def _get_resource(self, data, parsed_args):
        data = super(ShowIndex, self)._get_resource(data, parsed_args)
        index_name = parsed_args.index_name
        for index in data:
            if index['index_name'] == parsed_args.index_name:
                data = index
                break
        else:
            msg = _('Error. Index "%s" is not found in table "%s"')
            msg %= (index_name, parsed_args.name)
            raise exceptions.MagnetoDBClientException(msg)
        return data

    def add_known_arguments(self, parser):
        super(ShowIndex, self).add_known_arguments(parser)
        parser.add_argument(
            'index_name', metavar='INDEX_NAME',
            help=_('Name of index to describe'))


class CreateTable(magnetodbv1.CreateCommand):
    """Create a table for a given tenant."""

    resource = 'Table'
    log = logging.getLogger(__name__ + '.CreateTable')

    def args2body(self, parsed_args):
        body = {'table': {
            'name': parsed_args.name, }}
        magnetodbv1.update_dict(parsed_args, body['table'],
                                ['shared', 'tenant_id'])
        return body


class DeleteTable(magnetodbv1.DeleteCommand):
    """Delete a given table."""

    log = logging.getLogger(__name__ + '.DeleteTable')
    resource = 'table'
