import sys, os, subprocess, logging
from pgdb import connect, DatabaseError

logger = logging.getLogger("hdb_logger")


def check_executables():
        """
        Check if the necessary executables are available on PATH
        """
        run_cmd("which pg_dump")
        run_cmd("which pg_dumpall")


def error_logger(error):
        """
        If called it prints the error and exit
        :param error: the error message from the exception
        """
        # The error message to be printed when called.
        logger.error("Found exception in executing the command, "
                     "the error message received from the command is below, "
                     "aborting the script ...")
        logger.error(error)
        sys.exit(2)


def set_connection(dbname, host, port, username, password):
        """Set a new connection object for this backup
        :param:
            dbname - Database name
            host - Hostname
            port - Port number
            username - Username
            password - Password
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


def get_env():
        """
        Get the OS environment parameters
        """
        logger.debug("Get the OS environment variables")

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
        @:param
            cmd - command to be executed
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
