import sys, os, subprocess, logging
from pgdb import connect, DatabaseError

logger = logging.getLogger("hdb_logger")


def check_executables():
    """
    Check if the necessary executables are available on PATH
    """
    run_cmd("which pg_dump")
    run_cmd("which pg_dumpall")
    run_cmd("which pg_restore")


def error_logger(error):
    """
    If called it prints the error and exit
    :param error: the error message from the exception
    """
    # The error message to be printed when called.
    logger.error("Found exception in executing the command, "
                 "the error message received from the command is below")
    logger.error(error)
    logger.error("aborting the script ...")
    sys.exit(2)


def set_connection(dbname, host, port, username, password):
    """Set a new connection object for this backup
    :param:
        dbname      - Database name
        host        - Hostname
        port        - Port number
        username    - Username
        password    - Password
    :return:
        Conn   - Connection to the database
        Cursor - Cursor to execute the query
    """

    logger.debug("Attempting to create a connection to the database.")
    logger.debug("Parameters :- Database name: {0}, hostname: {1}, port: {2}, user: {3}".format(
            dbname, host, port, username
    ))
    try:
        conn = connect(
                database=dbname,
                host=host + ':' + str(port),
                user=username,
                password=password
        )
        cursor = conn.cursor()
    except DatabaseError, e:
        error_logger(e)

    return conn, cursor


def get_directory(base_directory, backup_id, dbname):
    """
    Prepare HDFS folders to backup the given database
    :return:
        Metadata Directory , Data directory
    """

    # Prepare the backup directory
    logger.debug("Preparing the backup folders")
    backup_base = base_directory
    backup_id = backup_id
    backup_db = dbname
    metadata_backup_dir = backup_base + '/' + str(backup_id) + '/' + backup_db + '/metadata'
    data_backup_dir = backup_base + '/' + str(backup_id) + '/' + backup_db + '/data'
    return metadata_backup_dir, data_backup_dir


def ext_table_sql_generator(create_ext, insert_ext, table, ext_schema, pxf_port, data_dir):
    """
    This method is responsible for creating all the external tables used to dump the data from the internal tables
    :param:
        create_ext  - Skeleton for create external table
        insert_ext  - Skeleton for Insert
        table       - table name (i.e in the format schema-name.table-name)
        ext_schema  - Schema name where the external table will be created.
        pxf_port    - pxf port number
        data_dir    - Data directory location
    :return: Create External Table SQL Query , Insert SQL Query
    """
    # Split the object into schema and relation name
    schema = table[0].split('.')[0]
    relation = table[0].split('.')[1]

    # Built the create external table query
    create_external_table_query = create_ext.format(
            ext_schema,
            relation,
            table[0],
            pxf_port,
            data_dir,
            schema.replace('"', ''),
            relation.replace('"', '')
    )

    # Built insert into external table query
    insert_external_table_query = insert_ext.format(
            ext_schema,
            relation,
            table[0]
    )

    return create_external_table_query, insert_external_table_query


def get_env():
    """
    Get the OS environment parameters
    :return: OS Environment
    """
    logger.debug("Get the OS environment variables")

    # Get the OS environment
    env = dict(os.environ)

    # we want to assure a consistent environment
    if 'PGOPTIONS' in env:
        del env['PGOPTIONS']

    if 'GPHOME' not in env:
        error_logger("The path doesnt have GPHOME, source the hawq environment path file and try again")

    return env


def run_cmd(cmd, popen_kwargs=None):
    """
    Run the command thrown at it by opening a shell and if encountered any error
    exit by displaying the command that failed.
    :param:  cmd - command to be executed
    :return: pipe
    """

    logger.debug("Attempting to run the command: \"{0}\"".format(
            cmd
    ))
    # Get the environment
    env = get_env()

    # Execute the commands
    pipe = subprocess.Popen(cmd, shell=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = pipe.communicate()

    # if the command execution fail, throw error
    if pipe.returncode > 0 or err:
        error_logger(err)

    return pipe


def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration    - Required  : current iteration (Int)
        total        - Required  : total iterations (Int)
        prefix       - Optional  : prefix string (Str)
        suffix       - Optional  : suffix string (Str)
        decimals     - Optional  : positive number of decimals in percent complete (Int)
        bar_length   - Optional  : character length of bar (Int)
    """
    format_str = "{0:." + str(decimals) + "f}"
    percents = format_str.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '#' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write('\r%s (%s/%s) |%s| %s%s %s' % (prefix, iteration, total, bar, percents, '%', suffix)),
    sys.stdout.flush()
    if iteration == total:
        sys.stdout.write('\n')
        sys.stdout.flush()
