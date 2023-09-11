"""Initial starting point from where all type of ingest start."""
from __future__ import annotations

import argparse
import glob
import logging
import sys
from logging import info

from reader.license_mapping_reader import LicenseMappingReader
from reader.lu_courses_reader import LuCoursesReader
from reader.role_permission_reader import RolePermissionReader
from reader.self_audit_reader import SelfAuditReader
from reader.self_audit_reader_v2 import SelfAuditReaderV2
from reader.sop_reader import SOPReader
from reader.zip_code_reader import ZipCodeReader
from util.validation import Validation


logging.basicConfig(
    format='INFO: %(message)s', level=logging.DEBUG,
    stream=sys.stdout,
)

v = Validation()


def get_file_processor(file_type='profile'):
    """To get file processor object."""
    category = {
        'role-permission': RolePermissionReader(),
        'sop-reader': SOPReader(),
        'zip-code': ZipCodeReader(),
        'self-audit': SelfAuditReader(),
        'self-audit-v2': SelfAuditReaderV2(),
        'license-mapping': LicenseMappingReader(),
        'lu-courses': LuCoursesReader(),
    }
    # need to set dynamically class call
    return category[file_type]


def main():
    """Call from direct execution of script."""
    arg_parser = argparse.ArgumentParser(
        description='Run the cold start scripts',
    )
    file_type_help = 'Provide type of ingestions.' \
        '\n\n Available type:' \
        'role-permission/zip-code/licence/audit/sop-reader/license-mapping/lu-courses'
    # add the arguments to the parser
    arg_parser.add_argument(
        '-f', '--file', required=False,
        type=v.check_file, help='Provide Xlsx file',
    )
    arg_parser.add_argument(
        '-t', '--type', required=True,
        type=v.check_ingestion_type,
        help=file_type_help,
    )
    arg_parser.add_argument(
        '-d', '--directory', required=False,
        type=v.check_directory,
        help='Provide valid directory path',
    )
    arg_parser.add_argument(
        '-c', '--check-file',
        required=False, help='Check the file content',
    )

    args = vars(arg_parser.parse_args())
    # Get the object of required processor
    instance = get_file_processor(args['type'].lower())
    info('File process for type : ' + args['type'])
    if args['directory'] is not None:
        files = glob.glob(args['directory']+'/*.xlsx')
        i = 0
        for file in files:
            print('Processing file : ' + file)
            if i == 0:
                i += 1
                if args['check_file'] is not None:
                    instance.file_validators(file_name=file)
            else:
                if args['check_file'] is not None:
                    instance.file_validators(file_name=file)
        instance.file_validators(file_name='', output=1)
    else:
        print('Processing file : ' + args['file'])
        if args['check_file'] is not None:
            instance.file_validators(file_name=args['file'])
        instance.file_validators(file_name=args['file'], output=1)
    return True


if __name__ == '__main__':
    main()
