# Copyright 2014 Symantec Corporation
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

"""
Command-line interface to the MagnetoDB streaming APIs
"""

from __future__ import print_function

import sys

from magnetodbclient.common import exceptions as exc
from magnetodbclient.magnetodb.v1 import item
from magnetodbclient.openstack.common.gettextutils import _
from magnetodbclient.openstack.common import strutils
from magnetodbclient import shell


API_NAME = 'kv-streaming'
VERSION = '1'
API_VERSION = '1'

COMMAND_V1 = {
    'upload': item.BulkLoad,
}

COMMANDS = {'1': COMMAND_V1}


class MagnetoDBStreamingShell(shell.MagnetoDBShell):

    def _add_specific_args(self, parser):
        parser.add_argument(
            '--service-type', metavar='<service-type>',
            default=shell.env('OS_KEYVALUE_SERVICE_TYPE',
                              default='kv-streaming'),
            help=_('Defaults to env[OS_KEYVALUE_SERVICE_TYPE].'))
        return parser


def main(argv=sys.argv[1:]):
    try:
        return MagnetoDBStreamingShell(API_VERSION, API_NAME, COMMANDS).run(
            map(strutils.safe_decode, argv))
    except exc.MagnetoDBClientException:
        return 1
    except Exception as e:
        print(unicode(e))
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
