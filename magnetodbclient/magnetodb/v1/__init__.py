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
import re

from cliff.formatters import table as cliff_table
from cliff import lister
from cliff import show

from magnetodbclient.common import command
from magnetodbclient.common import exceptions
from magnetodbclient.common import utils
from magnetodbclient.openstack.common.gettextutils import _

HEX_ELEM = '[0-9A-Fa-f]'
UUID_PATTERN = '-'.join([HEX_ELEM + '{8}', HEX_ELEM + '{4}',
                         HEX_ELEM + '{4}', HEX_ELEM + '{4}',
                         HEX_ELEM + '{12}'])


def _get_resource_plural(resource):
    return resource + 's'


def find_resourceid_by_id(client, resource, resource_id):
    resource_plural = _get_resource_plural(resource, client)
    obj_lister = getattr(client, "list_%s" % resource_plural)
    # perform search by id only if we are passing a valid UUID
    match = re.match(UUID_PATTERN, resource_id)
    collection = resource_plural
    if match:
        data = obj_lister(id=resource_id, fields='id')
        if data and data[collection]:
            return data[collection][0]['id']
    not_found_message = (_("Unable to find %(resource)s with id "
                           "'%(id)s'") %
                         {'resource': resource, 'id': resource_id})
    # 404 is used to simulate server side behavior
    raise exceptions.MagnetoDBClientException(
        message=not_found_message, status_code=404)


def _find_resourceid_by_name(client, resource, name):
    resource_plural = _get_resource_plural(resource, client)
    obj_lister = getattr(client, "list_%s" % resource_plural)
    data = obj_lister(name=name, fields='id')
    collection = resource_plural
    info = data[collection]
    if len(info) > 1:
        raise exceptions.MagnetoDBClientNoUniqueMatch(resource=resource,
                                                      name=name)
    elif len(info) == 0:
        not_found_message = (_("Unable to find %(resource)s with name "
                               "'%(name)s'") %
                             {'resource': resource, 'name': name})
        # 404 is used to simulate server side behavior
        raise exceptions.MagnetoDBClientException(
            message=not_found_message, status_code=404)
    else:
        return info[0]['id']


def find_resourceid_by_name_or_id(client, resource, name_or_id):
    try:
        return find_resourceid_by_id(client, resource, name_or_id)
    except exceptions.MagnetoDBClientException:
        return _find_resourceid_by_name(client, resource, name_or_id)


def add_show_list_common_argument(parser):
    parser.add_argument(
        '-D', '--show-details',
        help=_('Show detailed info'),
        action='store_true',
        default=False, )
    parser.add_argument(
        '--show_details',
        action='store_true',
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--fields',
        help=argparse.SUPPRESS,
        action='append',
        default=[])
    parser.add_argument(
        '-F', '--field',
        dest='fields', metavar='FIELD',
        help=_('Specify the field(s) to be returned by server,'
               ' can be repeated'),
        action='append',
        default=[])


def add_pagination_argument(parser):
    parser.add_argument(
        '-P', '--page-size',
        dest='page_size', metavar='SIZE', type=int,
        help=_("Specify retrieve unit of each request, then split one request "
               "to several requests"),
        default=None)


def add_sorting_argument(parser):
    parser.add_argument(
        '--sort-key',
        dest='sort_key', metavar='FIELD',
        action='append',
        help=_("Sort list by specified fields (This option can be repeated), "
               "The number of sort_dir and sort_key should match each other, "
               "more sort_dir specified will be omitted, less will be filled "
               "with asc as default direction "),
        default=[])
    parser.add_argument(
        '--sort-dir',
        dest='sort_dir', metavar='{asc,desc}',
        help=_("Sort list in specified directions "
               "(This option can be repeated)"),
        action='append',
        default=[],
        choices=['asc', 'desc'])


def is_number(s):
    try:
        float(s)  # for int, long and float
    except ValueError:
        try:
            complex(s)  # for complex
        except ValueError:
            return False

    return True


def _process_previous_argument(current_arg, _value_number, current_type_str,
                               _list_flag, _values_specs, _clear_flag,
                               values_specs):
    if current_arg is not None:
        if _value_number == 0 and (current_type_str or _list_flag):
                # This kind of argument should have value
                raise exceptions.CommandError(
                    _("Invalid values_specs %s") % ' '.join(values_specs))
        if _value_number > 1 or _list_flag or current_type_str == 'list':
            current_arg.update({'nargs': '+'})
        elif _value_number == 0:
            if _clear_flag:
                # if we have action=clear, we use argument's default
                # value None for argument
                _values_specs.pop()
            else:
                # We assume non value argument as bool one
                current_arg.update({'action': 'store_true'})


def parse_args_to_dict(values_specs):
    '''It is used to analyze the extra command options to command.

    '''

    # values_specs for example: '-- --tag x y --key1 type=int value1'
    # -- is a pseudo argument
    values_specs_copy = values_specs[:]
    if values_specs_copy and values_specs_copy[0] == '--':
        del values_specs_copy[0]
    # converted ArgumentParser arguments for each of the options
    _options = {}
    # the argument part for current option in _options
    current_arg = None
    # the string after remove meta info in values_specs
    # for example, '--tag x y --key1 value1'
    _values_specs = []
    # record the count of values for an option
    # for example: for '--tag x y', it is 2, while for '--key1 value1', it is 1
    _value_number = 0
    # list=true
    _list_flag = False
    # action=clear
    _clear_flag = False
    # the current item in values_specs
    current_item = None
    # the str after 'type='
    current_type_str = None
    for _item in values_specs_copy:
        if _item.startswith('--'):
            # Deal with previous argument if any
            _process_previous_argument(
                current_arg, _value_number, current_type_str,
                _list_flag, _values_specs, _clear_flag, values_specs)

            # Init variables for current argument
            current_item = _item
            _list_flag = False
            _clear_flag = False
            current_type_str = None
            if "=" in _item:
                _value_number = 1
                _item = _item.split('=')[0]
            else:
                _value_number = 0
            if _item in _options:
                raise exceptions.CommandError(
                    _("Duplicated options %s") % ' '.join(values_specs))
            else:
                _options.update({_item: {}})
            current_arg = _options[_item]
            _item = current_item
        elif _item.startswith('type='):
            if current_arg is None:
                raise exceptions.CommandError(
                    _("Invalid values_specs %s") % ' '.join(values_specs))
            if 'type' not in current_arg:
                current_type_str = _item.split('=', 2)[1]
                current_arg.update({'type': eval(current_type_str)})
                if current_type_str == 'bool':
                    current_arg.update({'type': utils.str2bool})
                elif current_type_str == 'dict':
                    current_arg.update({'type': utils.str2dict})
                continue
        elif _item == 'list=true':
            _list_flag = True
            continue
        elif _item == 'action=clear':
            _clear_flag = True
            continue

        if not _item.startswith('--'):
            # All others are value items
            # Make sure '--' occurs first and allow minus value
            if (not current_item or '=' in current_item or
                _item.startswith('-') and not is_number(_item)):
                raise exceptions.CommandError(
                    _("Invalid values_specs %s") % ' '.join(values_specs))
            _value_number += 1

        _values_specs.append(_item)

    # Deal with last one argument
    _process_previous_argument(
        current_arg, _value_number, current_type_str,
        _list_flag, _values_specs, _clear_flag, values_specs)

    # populate the parser with arguments
    _parser = argparse.ArgumentParser(add_help=False)
    for opt, optspec in _options.iteritems():
        _parser.add_argument(opt, **optspec)
    _args = _parser.parse_args(_values_specs)

    result_dict = {}
    for opt in _options.iterkeys():
        _opt = opt.split('--', 2)[1]
        _opt = _opt.replace('-', '_')
        _value = getattr(_args, _opt)
        result_dict.update({_opt: _value})
    return result_dict


def _merge_args(qCmd, parsed_args, _extra_values, value_specs):
    """Merge arguments from _extra_values into parsed_args.

    If an argument value are provided in both and it is a list,
    the values in _extra_values will be merged into parsed_args.

    @param parsed_args: the parsed args from known options
    @param _extra_values: the other parsed arguments in unknown parts
    @param values_specs: the unparsed unknown parts
    """
    temp_values = _extra_values.copy()
    for key, value in temp_values.iteritems():
        if hasattr(parsed_args, key):
            arg_value = getattr(parsed_args, key)
            if arg_value is not None and value is not None:
                if isinstance(arg_value, list):
                    if value and isinstance(value, list):
                        if (not arg_value or
                            type(arg_value[0]) == type(value[0])):
                            arg_value.extend(value)
                            _extra_values.pop(key)


def update_dict(obj, dict, attributes):
    """Update dict with fields from obj.attributes

    :param obj: the object updated into dict
    :param dict: the result dictionary
    :param attributes: a list of attributes belonging to obj
    """
    for attribute in attributes:
        if hasattr(obj, attribute) and getattr(obj, attribute) is not None:
            dict[attribute] = getattr(obj, attribute)


class TableFormater(cliff_table.TableFormatter):
    def emit_list(self, column_names, data, stdout, parsed_args):
        if column_names:
            super(TableFormater, self).emit_list(column_names, data, stdout,
                                                 parsed_args)
        else:
            stdout.write('\n')


class MagnetoDBCommand(command.OpenStackCommand):
    api = 'keyvalue'
    log = logging.getLogger(__name__ + '.MagnetoDBCommand')
    values_specs = []
    excluded_rows = ()
    _formatters = {}
    resource_path = ()
    required_args = ()
    json_indent = None
    success_message = ''

    def get_client(self):
        return self.app.client_manager.magnetodb

    def check_required_args(self, parsed_args):
        for arg in self.required_args:
            if not getattr(parsed_args, arg):
                param_name = '--%s' % arg.replace('_', '-')
                msg = _("'%s' is a required parameter") % param_name
                raise exceptions.MagnetoDBCLIError(message=msg)

    def get_parser(self, prog_name):
        parser = super(MagnetoDBCommand, self).get_parser(prog_name)
        self.add_known_arguments(parser)
        return parser

    def _get_info(self, data, parsed_args):
        for path in self.resource_path:
            data = data.get(path, self.default_info)
            if not data:
                data = self.default_info
        return data

    def format_output_data(self, info):
        # Modify data to make it more readable
        for k, v in info.iteritems():
            if isinstance(v, list):
                value = '\n'.join(utils.dumps(
                    i, indent=self.json_indent) if isinstance(i, dict)
                    else str(i) for i in v)
                info[k] = value
            elif isinstance(v, dict):
                value = utils.dumps(v, indent=self.json_indent)
                info[k] = value
            elif v is None:
                info[k] = ''

    def exclude_rows(self, info):
        for row in self.excluded_rows:
            try:
                del info[row]
            except KeyError:
                pass

    def add_known_arguments(self, parser):
        pass

    def args2body(self, parsed_args):
        return {}


class CreateCommand(MagnetoDBCommand, show.ShowOne):
    """Create a resource for a given tenant

    """

    api = 'keyvalue'
    resource = None
    log = None
    success_message = _('Created a new %s:')
    default_info = {'': ''}

    def format_output_data(self, info):
        for k, v in info.iteritems():
            if k in self._formatters:
                info[k] = self._formatters[k](v)
        super(CreateCommand, self).format_output_data(info)

    def get_data(self, parsed_args):
        self.log.debug('get_data(%s)' % parsed_args)
        self.check_required_args(parsed_args)
        magnetodb_client = self.get_client()
        _extra_values = parse_args_to_dict(self.values_specs)
        _merge_args(self, parsed_args, _extra_values,
                    self.values_specs)
        body = self.args2body(parsed_args)
        obj_creator = getattr(magnetodb_client, self.method)
        if getattr(parsed_args, 'name', None):
            data = obj_creator(parsed_args.name, body)
        else:
            data = obj_creator(body)
        info = self._get_info(data, parsed_args)
        self.format_output_data(info)
        self.exclude_rows(info)

        print(self.success_message % self.resource,
              file=self.app.stdout)
        return zip(*sorted(info.iteritems()))


class UpdateCommand(MagnetoDBCommand, show.ShowOne):
    """Update resource's information
    """

    api = 'keyvalue'
    resource = None
    log = None
    default_info = {'': ''}

    def run(self, parsed_args):
        self.log.debug('run(%s)', parsed_args)
        self.check_required_args(parsed_args)
        magnetodb_client = self.get_client()
        body = self.args2body(parsed_args)
        obj_updator = getattr(magnetodb_client, self.method)
        data = obj_updator(parsed_args.name, body)

        info = self._get_info(data, parsed_args)
        self.format_output_data(info)

        print((_('Updated %(resource)s: %(name)s') %
               {'name': parsed_args.name, 'resource': self.resource}),
              file=self.app.stdout)
        return zip(*sorted(info.iteritems()))


class DeleteCommand(MagnetoDBCommand):
    """Delete a given resource

    """

    api = 'keyvalue'
    resource = None
    log = None

    def get_parser(self, prog_name):
        parser = super(DeleteCommand, self).get_parser(prog_name)
        help_str = _('Name of %s to delete')
        parser.add_argument(
            'name', metavar=self.resource.upper(),
            help=help_str % self.resource)
        return parser

    def run(self, parsed_args):
        self.log.debug('run(%s)', parsed_args)
        self.check_required_args(parsed_args)
        magnetodb_client = self.get_client()
        obj_deleter = getattr(magnetodb_client, self.method)
        _name = parsed_args.name
        obj_deleter(_name)
        print((_('Deleted %(resource)s: %(name)s')
               % {'name': _name,
                  'resource': self.resource}),
              file=self.app.stdout)
        return


class ListCommand(MagnetoDBCommand, lister.Lister):
    """List resources that belong to a given tenant

    """

    api = 'keyvalue'
    resource = None
    log = None
    list_columns = []
    unknown_parts_flag = True
    pagination_support = False
    sorting_support = False
    default_info = []

    def get_parser(self, prog_name):
        parser = super(ListCommand, self).get_parser(prog_name)
        add_show_list_common_argument(parser)
        if self.pagination_support:
            add_pagination_argument(parser)
        if self.sorting_support:
            add_sorting_argument(parser)
        return parser

    def args2search_opts(self, parsed_args):
        search_opts = {}
        self.check_required_args(parsed_args)
        fields = parsed_args.fields
        if parsed_args.fields:
            search_opts.update({'fields': fields})
        if parsed_args.show_details:
            search_opts.update({'verbose': 'True'})
        return search_opts

    def call_server(self, magnetodb_client, search_opts, parsed_args, body):
        obj_lister = getattr(magnetodb_client, self.method)
        data = obj_lister(**search_opts)
        return data

    def retrieve_list(self, parsed_args):
        """Retrieve a list of resources from MagnetoDB server"""
        magnetodb_client = self.get_client()
        _extra_values = parse_args_to_dict(self.values_specs)
        _merge_args(self, parsed_args, _extra_values,
                    self.values_specs)
        search_opts = self.args2search_opts(parsed_args)
        search_opts.update(_extra_values)
        body = self.args2body(parsed_args)
        if self.pagination_support:
            page_size = parsed_args.page_size
            if page_size:
                search_opts.update({'limit': page_size})
        if self.sorting_support:
            keys = parsed_args.sort_key
            if keys:
                search_opts.update({'sort_key': keys})
            dirs = parsed_args.sort_dir
            len_diff = len(keys) - len(dirs)
            if len_diff > 0:
                dirs += ['asc'] * len_diff
            elif len_diff < 0:
                dirs = dirs[:len(keys)]
            if dirs:
                search_opts.update({'sort_dir': dirs})
        data = self.call_server(magnetodb_client, search_opts, parsed_args,
                                body)
        return data

    def extend_list(self, data, parsed_args):
        """Update a retrieved list.
        """
        pass

    def setup_columns(self, info, parsed_args):
        _columns = len(info) > 0 and sorted(info[0].keys()) or []
        if not _columns:
            # clean the parsed_args.columns so that cliff will not break
            parsed_args.columns = []
        elif parsed_args.columns:
            _columns = [x for x in parsed_args.columns if x in _columns]
        elif self.list_columns:
            _columns = self.list_columns
        return (_columns, (utils.get_item_properties(
            s, _columns, formatters=self._formatters, )
            for s in info), )

    def get_data(self, parsed_args):
        self.log.debug('get_data(%s)', parsed_args)
        self.check_required_args(parsed_args)
        data = self.retrieve_list(parsed_args)
        info = self._get_info(data, parsed_args)
        if self.success_message:
            print(self.success_message)
        self.extend_list(info, parsed_args)
        return self.setup_columns(info, parsed_args)


class ShowCommand(MagnetoDBCommand, show.ShowOne):
    """Show information of a given resource

    """

    api = 'keyvalue'
    resource = None
    log = None
    default_info = {'': ''}

    def get_parser(self, prog_name):
        parser = super(ShowCommand, self).get_parser(prog_name)
        add_show_list_common_argument(parser)
        return parser

    def format_output_data(self, info):
        for k, v in info.iteritems():
            if k in self._formatters:
                info[k] = self._formatters[k](v)
        super(ShowCommand, self).format_output_data(info)

    def call_server(self, magnetodb_client, name, parsed_args, body):
        obj_shower = getattr(magnetodb_client, self.method)
        data = obj_shower(name)
        return data

    def get_data(self, parsed_args):
        self.log.debug('get_data(%s)', parsed_args)
        self.check_required_args(parsed_args)
        magnetodb_client = self.get_client()
        _name = getattr(parsed_args, 'name', None)
        body = self.args2body(parsed_args)
        data = self.call_server(magnetodb_client, _name, parsed_args, body)
        info = self._get_info(data, parsed_args)
        self.format_output_data(info)
        self.exclude_rows(info)
        if self.success_message:
            print(self.success_message)
        return zip(*sorted(info.iteritems()))
