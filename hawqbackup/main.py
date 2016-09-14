import sys
import argparse
import logging
import logging.handlers
import pgdb

import hawqbackup
import backup
import restore

from os.path import expanduser


def parseargs(args):
    logger = logging.getLogger("hdb_backup")

    # Global options
    parser = argparse.ArgumentParser(prog='hawq-backup', # usage="%(prog)s (backup | restore) [options] -d database",
                                     description='This tool aims to help automating HDB backups and '
                                                 'restore tasks. This tool is capable of parallel '
                                                 'backup and restore a database to/from HDFS.',
                                     epilog='Source code available at https://github.com/Zerpet/hdb-backup',
                                     add_help=False)
    parser.add_argument('-?', '--help', action='help', help='Prints this message')

    shared_parser = argparse.ArgumentParser(add_help=False)
    shared_parser.add_argument('-?', '--help', action='help', help='Prints this message')
    shared_parser.add_argument('--debug', action='store_true', help='Enables debug logging')
    shared_parser.add_argument('-B', '--parallel', type=int, default=4, help='Number of parallel workers (Default: 4)')
    shared_parser.add_argument('-v', '--version', action='version', version=' %(prog)s ' + hawqbackup.__version__)

    # Connection parameters
    shared_parser.add_argument('-U', '--username', default='gpadmin',
                               help='User to use in the connection to the database')
    shared_parser.add_argument('-p', '--port', default=5432, type=int, help='Port where the database is listening')
    shared_parser.add_argument('-h', '--host', default='localhost', help='Host where the database is running')
    shared_parser.add_argument('-w', '--password', help='Password to connect to the database')
    # Database is always required
    shared_parser.add_argument('-d', '--database', required=True, help='Database to connect to')

    shared_parser.add_argument('--schema-only', dest='schemaonly', action='store_true',
                               help='Backup/restore only the table structure, do not backup/restore data')
    shared_parser.add_argument('--data-only', dest='dataonly', action='store_true',
                               help='Backup/restore data only, do not backup/restore table structure')

    subparsers = parser.add_subparsers(dest='command')

    # Backup specific options
    backup_parser = subparsers.add_parser('backup', parents=[shared_parser], add_help=False, help='Backup a database into HDFS')
    backup_options_group = backup_parser.add_mutually_exclusive_group()
    # TODO: check schema if it has more than one dot
    backup_options_group.add_argument('-s', '--schema', help='Backup only tables in this schema')
    backup_options_group.add_argument('-S', '--schema-list', dest='schema_list',
                                      help='Coma separated list of schemas to backup tables from')
    backup_options_group.add_argument('-t', '--table', help='Backup just this table')

    # Restore specific options
    restore_parser = subparsers.add_parser('restore', add_help=False, parents=[shared_parser],
                                           help='Restore a database from HDFS')
    restore_parser.add_argument('backupid', metavar='timestamp', nargs=1, type=long)

    options_object = parser.parse_args(args)

    if options_object.database is None:
        logger.error("You have to specify a database to connect to")
        parser.exit(2)

    if options_object.debug is True:
        logger.setLevel(10)

    return options_object


def main(args):
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

    logger.info("Starting HDB backup utility")

    if cmdline_args.command == 'backup':
        pass
    else:
        pass

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])
