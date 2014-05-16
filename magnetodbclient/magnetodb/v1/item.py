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


class GetItem(magnetodbv1.ShowCommand):
    """Gets item from a given table by key."""

    resource = 'item'
    resource_path = ('item',)
    method = 'get_item'
    log = logging.getLogger(__name__ + '.GetItem')

    def add_known_arguments(self, parser):
        help_str = _('Name of table to look up')
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=help_str)
        parser.add_argument(
            '--request-file', metavar='FILE', dest='request_file_name',
            help=_('File that contains item description to put in table'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file_name)


class PutItem(magnetodbv1.CreateCommand):
    """Puts item to a given table."""

    resource = 'item'
    resource_path = ()
    method = 'put_item'
    log = logging.getLogger(__name__ + '.PutItem')

    def add_known_arguments(self, parser):
        help_str = _('Name of table to put item in')
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=help_str)
        parser.add_argument(
            '--request-file', metavar='FILE', dest='request_file_name',
            help=_('File that contains item description to put in table'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file_name)


class DeleteItem(magnetodbv1.CreateCommand):
    """Deletes item from a given table."""

    resource = 'item'
    method = 'delete_item'
    resource_path = ('attributes',)
    success_message = _('Deleted %s:')
    log = logging.getLogger(__name__ + '.DeleteItem')

    def add_known_arguments(self, parser):
        help_str = _('Name of table to delete item from')
        parser.add_argument(
            'name', metavar='TABLE_NAME',
            help=help_str)
        parser.add_argument(
            '--request-file', metavar='FILE', dest='request_file_name',
            help=_('File that contains item key description'))

    def args2body(self, parsed_args):
        return utils.get_file_contents(parsed_args.request_file_name)
