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

import argparse
import logging

from magnetodbclient.common import exceptions
from magnetodbclient.magnetodb import v1 as magnetodb
from magnetodbclient.openstack.common.gettextutils import _


class CreateTable(magnetodb.CreateCommand):
    """Create a table for a given tenant."""

    resource = 'table'
    log = logging.getLogger(__name__ + '.CreateTable')

    def add_known_arguments(self, parser):
        parser.add_argument(
            '--description-file', metavar='FILE', dest='desc_file_name',
            help=_('File that contains table description to create'))

    def args2body(self, parsed_args):
        body = {'file': parsed_args.desc_file_name, }
        return body


class DeleteTable(magnetodb.DeleteCommand):
    """Delete a given table."""

    log = logging.getLogger(__name__ + '.DeleteTable')
    resource = 'table'
