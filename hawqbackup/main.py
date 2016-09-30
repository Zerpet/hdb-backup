import sys
import argparse
import logging
import logging.handlers

import hawqbackup
import backup
import restore

from os.path import expanduser


def parseargs(args):
    logger = logging.getLogger("hdb_backup")

    # Global options
    parser = argparse.ArgumentParser(prog='hawq-backup',
                                     description='This tool aims to help automating HDB backups and '
                                                 'restore tasks. This tool is capable of parallel '
                                                 'backup and restore a database to/from HDFS.',
                                     epilog='Source code available at https://github.com/Zerpet/hdb-backup',
                                     add_help=False)
    parser.add_argument('-?', '--help', action='help', help='Prints this message')

    shared_parser = argparse.ArgumentParser(add_help=False)
    shared_parser.add_argument('-?', '--help', action='help', help='Prints this message')
    shared_parser.add_argument('--debug', action='store_true', help='Enables debug logging')
    shared_parser.add_argument('-q', '--quiet', action='store_true', help='Do not print to standard error. Log file'
                                                                          ' is always logged')
    shared_parser.add_argument('-v', '--version', action='version', version=' %(prog)s ' + hawqbackup.__version__)

    # Connection parameters
    shared_parser.add_argument('-U', '--username', default='gpadmin',
                               help='User to use in the connection to the database')
    shared_parser.add_argument('-p', '--port', default=5432, type=int, help='Port where the database is listening')
    shared_parser.add_argument('-h', '--host', default='localhost', help='Host where the database is running')
    shared_parser.add_argument('-w', '--password', help='Password to connect to the database')
    # Database is always required
    shared_parser.add_argument('-d', '--database', required=True, help='Database to connect to')
    shared_parser.add_argument('-F', '--force', default=False, action='store_true',
                               help='Drop hawqbackup schema if it exists')
    shared_parser.add_argument('--include-roles', dest='include_roles', default=False, action='store_true',
                               help='Include user roles and resource queues')
    shared_parser.add_argument('-y', '--yes', action='store_true', default=False, help='Assume Yes to every prompt')

    schema_or_data_group = shared_parser.add_mutually_exclusive_group()
    schema_or_data_group.add_argument('--schema-only', dest='schema_only', action='store_true',
                                      help='Backup/restore only the table structure, do not backup/restore data')
    schema_or_data_group.add_argument('--data-only', dest='data_only', action='store_true',
                                      help='Backup/restore data only, do not backup/restore table structure')

    subparsers = parser.add_subparsers(dest='command')

    # Backup specific options
    backup_parser = subparsers.add_parser('backup', parents=[shared_parser], add_help=False,
                                          help='Backup a database into HDFS')
    backup_options_group = backup_parser.add_mutually_exclusive_group()
    backup_options_group.add_argument('-s', '--schema', help='Backup only tables in this schema. Accepts '
                                                             'comma-separated list for multiple')
    backup_options_group.add_argument('-t', '--table', help='Backup just this table. Accepts '
                                                            'comma-separated list for multiple')
    backup_options_group.add_argument('--exclude-table', dest='exclude_table',
                                      help='Do not include table "schema"."table" in the backup. Accepts '
                                           'comma-separated list for multiple tables')
    backup_options_group.add_argument('--exclude-schema', dest='exclude_schema',
                                      help='Do not include schema "schema" in the backup. Accepts '
                                           'comma-separated list for multiple schemas')

    # Restore specific options
    restore_parser = subparsers.add_parser('restore', add_help=False, parents=[shared_parser],
                                           help='Restore a database from HDFS')
    restore_parser.add_argument('-k', '--backup-id', dest='backup_id', metavar='201609220000', nargs=1, type=long,
                                required=True)
    restore_parser.add_argument('--target-database', dest='target_database',
                                help='Restore <database> into <target_database>. Useful if the name of original '
                                     'database does not match the new one')
    restore_parser.add_argument('--ignore-error', action='store_true', default=False, help='Ignore errors when '
                                                                                           'restoring metadata')

    # Input/output file exclusive group
    input_output_file_group = restore_parser.add_mutually_exclusive_group()
    input_output_file_group.add_argument('--output-to-file', action='store_true',
                                         help='Dump what this restore will do to a file. This file can be modified '
                                              'and used as input for --input-file option for a selective restore')
    input_output_file_group.add_argument('--input-file', dest='input_file',
                                         help='Input file for selective restore. This file has to be generated by '
                                              '--output-to-file option.')

    options_object = parser.parse_args(args)

    if options_object.database is None:
        logger.error("You have to specify a database to connect to")
        parser.exit(2)

    return options_object


def is_schema_name_valid(name):
    """
    This function helps to check that schema names or table names have proper format. Specifically, we have to check
    if the name has a dot. If it does, the name should be enclosed in double quotes.

    To check if a full table name (schema + table) is valid, use __is_table_name_valid()
    :param name: schema or table name to check
    :return: True if the name is valid; False else
    """
    # TODO: implement me
    return False


def is_table_name_valid(name):
    """
    Check if schema.table name is valid. It has to check if the full name has dots; if it does, only one dot should be
    outside double quotes
    :param name:
    :return:
    """

    if type(name) is not str:
        return False

    n_dots = name.count('.')

    # No schema name provided, failure. We need <schema>.<table>
    if n_dots == 0:
        return False

    elif n_dots == 1:
        return True

    # We have more than one dot. Names should be quoted
    else:
        import re
        valid1 = r'^"[a-zA-Z\.]+"\."[a-zA-Z\.]+"$'
        valid2 = r'^"[a-zA-Z\.]+"\.[a-zA-Z]+$'
        valid3 = r'^[a-zA-Z]+\."[a-zA-Z\.]+"$'

        return re.match(valid1, name) or re.match(valid2, name) or re.match(valid3, name) or False


def main(args):
    """
    Where all the magic happens
    :param args: sys.argv[1:]
    :return: 0 on success. 1 on invalid option/s in command line.
    """
    logging_format = "%(asctime)-15s - %(module)s.%(funcName)s - %(levelname)s - %(message)s"
    log_file_name = expanduser('~') + '/hawq_backup.log'

    logging_formatter = logging.Formatter(logging_format)

    rf_handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=128 * 1024 * 1024, backupCount=5)
    rf_handler.setFormatter(logging_formatter)

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(logging_formatter)

    logger = logging.getLogger("hdb_logger")
    logger.addHandler(rf_handler)
    logger.addHandler(stderr_handler)
    logger.setLevel(logging.INFO)

    cmdline_args = parseargs(args)

    if cmdline_args.debug is True:
        logger.setLevel(10)

    if cmdline_args.quiet:
        logger.removeHandler(stderr_handler)

    if cmdline_args.schema and not is_schema_name_valid(cmdline_args.schema):
        logger.error("The schema name '%s' is not valid. Make sure you use double quotes if your name contains dots"
                     % cmdline_args.schema)
        return 1

    logger.info("Starting HDB backup utility")

    if cmdline_args.command == 'backup':
        logger.debug("Initializing backup stage")
        hdb_backup = backup.HdbBackup()

        logger.debug("Setting options for backup")
        hdb_backup.set_vars(cmdline_args)

        hdb_backup.run_backup()

    else:
        logger.debug("Initializing restore stage")
        hdb_restore = restore.HDBRestore()

        logger.debug("Setting options for restore")
        hdb_restore.set_vars(cmdline_args)

        hdb_restore.run_restore()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
