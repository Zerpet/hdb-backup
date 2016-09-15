# import psycopg2, subprocess, os, datetime, sys, time
import subprocess, os, datetime, sys, time
# from hawqlogger import LOG


connection_obj = "aaa"
class HdbBackup:

    def __init__(self, connection_obj):
        """
        Create a HdbBackup object with a connection object
        :param connection_obj An initialized Psycopg2 connection object
        """
        # assert type(connection_obj) == psycopg2.extensions.connection
        self.conn = connection_obj
        self.backup_id = None
        self.username = None
        self.host = None
        self.port = 10432
        self.dbname = 'gpadmin'
        self.table = None
        self.schema = None
        self.exclude_table = None
        self.exclude_schema = None
        self.data_only = None
        self.schema_only = None
        self.clean = None
        self.no_privileges = None
        self.global_dump = None
        self.metadata_backup_dir = None
        self.data_backup_dir = None
        self.force = None
        self.create_database = None
        self.ext_schema_name = 'hawqbackup_schema'
        self.pxf_port = 51200
        self.drop_schema_skeleton = """ DROP SCHEMA IF EXISTS {0} CASCADE """
        self.create_schema_skeleton = """ CREATE SCHEMA {0} """
        self.lock_table_skeleton = """ LOCK TABLE {0} IN ACCESS SHARE MODE """
        self.create_external_table_skeleton = """ CREATE WRITABLE EXTERNAL TABLE {0}.{1} ( like {2} )
                                              LOCATION ('pxf://localhost:{3}{4}/{5}/{6}?profile=HdfsTextSimple')
                                              FORMAT 'TEXT' (DELIMITER = E'\\t') """
        self.insert_external_table_skeleton = """ INSERT INTO {0}.{1} SELECT * FROM {2} """

    def __get_env(self):
        """
        Get the OS environment parameters
        """
        if hasattr(self, 'env'):
            return self.env

        self.env = dict(os.environ)

        # we want to assure a consistent environment
        if 'PGOPTIONS' in self.env:
            del self.env['PGOPTIONS']

        if 'GPHOME' not in self.env:
            # TODO: to change the error
            print "the path doesnt have GPHOME, please source the hawq environment path file and try again"
            sys.exit(2)

        return self.env

    def __check_executables(self):
        """
        Check if the necessary executables are available on PATH
        """
        self.__run_cmd("which psql")
        self.__run_cmd("which pg_dump")
        self.__run_cmd("which pg_dumpall")
        self.__run_cmd("which gzip")

    def set_backup_id(self):
        """Set the backup ID to be used in this backup. The most common ID format is <year><month><day><hour><minute>
         For example, 201601011700 will be the standard for January 1st, 2016 at 5 PM.
        """
        self.backup_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return self.backup_id

    def __prepare_backup(self):
        """
        Prepare HDFS folders to backup the given database
        """
        # Set backup id
        self.set_backup_id()

        # Prepare the backup directory
        backup_base = "/hawq_backup"
        backup_id = self.backup_id
        backup_db = self.dbname
        self.metadata_backup_dir = backup_base + '/' + str(backup_id) + '/' + backup_db + '/metadata'
        self.data_backup_dir = backup_base + '/' + str(backup_id) + '/' + backup_db + '/data'

    def __backup_metadata(self):
        """
        Backup DDLs and metadata before dumping the data
        """
        # Backup file name
        ddl_file = self.metadata_backup_dir + '/hdb_dump_' + self.backup_id + '_ddl.dmp'
        global_file = self.metadata_backup_dir + '/hdb_dump_' + self.backup_id + '_global.dmp'

        # Metadata backup command creator
        pg_dump_cmd, pg_dumpall_cmd = self.__get_args(
            "pg_dump",
            "--schema-only",
            "--format=p"
        )
        pg_dump_cmd = ' '.join(pg_dump_cmd)
        pg_dump_cmd += ' | sudo -u hdfs hdfs dfs -put - {0}'.format(ddl_file)
        self.__run_cmd(pg_dump_cmd)

        if pg_dumpall_cmd:
            pg_dumpall_cmd += ' | sudo -u hdfs hdfs dfs -put - {0}'.format(global_file)
            self.__run_cmd(pg_dumpall_cmd)

    def __get_args(self, executable, *args):
        """
        compile all the executable and the arguments, combining with common arguments
        to create a full batch of command args
        @:param:
            executable - type of command
            *args      - other Keyword argument parameters
        """
        args = list(args)
        args.insert(0, executable)

        # If username supplied
        if self.username:
            args.append(
                    "--username={0}".format(self.username)
            )

        # If hostname supplied
        if self.host:
            args.append(
                    "--host={0}".format(self.host)
            )

        # If port supplied
        if self.port:
            args.append(
                    "--port={0}".format(self.port)
            )

        # If drop DDL needed
        if self.clean:
            args.append(
                    "--clean"
            )

        # If Grant/Revoke not needed
        if self.no_privileges:
            args.append(
                    "--no-privileges"
            )

        # If Created database needed
        if self.create_database:
            args.append(
                    "--create"
            )

        # If only table backup needed
        if self.table:
            args.append(
                    "--table={0}".format(
                            ' --table='.join(self.table.split(','))
                    )
            )

        # If only schema backup needed
        if self.schema:
            args.append(
                    "--schema={0}".format(
                            ' --schema='.join(self.schema.split(','))
                    )
            )

        # If any table needed to be excluded
        if self.exclude_table:
            args.append(
                    "--exclude-table={0}".format(
                            ' --exclude-table='.join(self.exclude_table.split(','))
                    )
            )

        # If any schema needed to be excluded
        if self.exclude_schema:
            args.append(
                    "--exclude-schema={0}".format(
                            ' --exclude-schema='.join(self.exclude_schema.split(','))
                    )
            )

        # If global dump requested or If this is a full database backup, then get all the global object else ignore
        if self.global_dump or not (self.table or self.schema or self.exclude_table or self.exclude_schema):
            pg_dumpall_cmd = "pg_dumpall --schema-only --globals-only"
        else:
            pg_dumpall_cmd = None

        # Pass the Database name to backup
        args.append(self.dbname)
        return args, pg_dumpall_cmd

    def __run_cmd(self, cmd, ignore_ret_code=False, popen_kwargs=None):
        """
        Run the command thrown at it by opening a shell and if encountered any error
        exit by displaying the command that failed.
        @:param
            cmd - command to be executed
        """
        # Get the environment
        env = self.__get_env()

        # Shell arguments
        kwargs = {
            'shell': True,
            'env': env
        }

        if cmd.startswith('which'):
            kwargs['stdout'] = subprocess.PIPE

        if popen_kwargs:
            kwargs.update(popen_kwargs)

        pipe = subprocess.Popen(
            cmd,

            **kwargs
        )

        # Execute the commands
        ret_code = pipe.wait()

        # if the command execution fail, throw error
        if not ignore_ret_code and ret_code > 0:
            raise RuntimeError('command "{0}" did not execute correctly'.format(cmd))

        return pipe

    def set_connection(self, connection_obj):
        """Set a new connection object for this backup
        :param connection_obj is a Psycopg2 connection object
        """
        assert type(connection_obj) == psycopg2.extensions.connection
        self.conn = connection_obj

    def __fetch_object_info(self):
        """
        This method is responsible for fetching all the table names (with schema) from a given database
        and based on the option passed it dynamically alters its condition..
        :param database is the name of the target database
        """
        # Main query skeleton
        query = """SELECT '"'
                           || nspname
                           || '"."'
                           || relname
                           || '"'
                    FROM   pg_namespace n
                           JOIN pg_class c
                             ON ( n.oid = c.relnamespace )
                    WHERE  n.nspname NOT IN ( 'pg_catalog', 'information_schema', 'pg_aoseg',
                                              'pg_bitmapindex',
                                              'pg_toast', 'gp_toolkit' )
                    AND c.relkind = 'r' and c.relstorage != 'x' """

        # If only selected table then add table include condition
        if self.table:
            query += """ AND c.oid in ('{0}'""".format(
                            '\'::regclass,\''.join(self.table.split(','))
                    ) + """::regclass)"""

        # If omit few table then add table exclude condition
        if self.exclude_table:
            query += """ AND c.oid not in ('{0}'""".format(
                            '\'::regclass,\''.join(self.exclude_table.split(','))
                    ) + """::regclass)"""

        # If only selected schema then add schema include condition
        if self.schema:
            query += """ AND n.nspname in ('{0}'""".format(
                            '\',\''.join(self.schema.split(','))
                    ) + """)"""

        # If omit few schema then add schema exclude condition
        if self.exclude_schema:
            query += """ AND n.nspname not in ('{0}'""".format(
                            '\',\''.join(self.exclude_schema.split(','))
                    ) + """)"""

        print query

    def __external_tables_activity_builder(self, table):
        """
        This method is responsible for creating all the external tables used to dump the data from the internal tables
        @:param: table - table name (i.e in the format schema-name.table-name)
        """
        # Split the object into schema and relation name
        schema = table.split('.')[0]
        relation = table.split('.')[1]

        # Built the create external table query
        create_external_table_query = self.create_external_table_skeleton.format(
            self.ext_schema_name,
            relation,
            table,
            self.pxf_port,
            self.data_backup_dir,
            schema.replace('"', ''),
            relation.replace('"', '')
        )

        # Built insert into external table query
        insert_external_table_query = self.insert_external_table_skeleton.format(
            self.ext_schema_name,
            relation,
            table
        )

        return create_external_table_query, insert_external_table_query

    def print_progress(self, iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
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

    def __backup_data(self):
        """
        Backup actual data into external tables. This method is to run the
        INSERT INTO ext_table SELECT * from internal_table
        """
        drop_schema = self.drop_schema_skeleton.format(self.ext_schema_name)
        create_schema = self.create_schema_skeleton.format(self.ext_schema_name)

        # In case the previous hawqbackup schema was not cleaned up and user supplied force then drop it
        if self.force:
            # print drop_schema_query
            pass

        # TODO: Check if the schema exists then we error out stating to use force.

        # TODO: run create_schema

        # tables = self.__fetch_object_info()
        tables = ['"aaaa"."ffff"', 'bbbb.ccccc', '"aawaa"."ffff"', 'bbwwebb.ccccc', '"aaweaa"."ffff"', 'bewebbb.ccccc', '"aaeweaa"."ffff"', 'ewew.ccccc', '"ew"."ffff"', 'ew.ccccc', '"aasdfaa"."ffff"', 'uty.ccccc','"vcvcxv"."ffff"', 'ret.ccccc', '"aaaaaaa"."ffff"', 'buuubbb.ccccc']
        total_tables = len(tables)

        # TODO: Make sure we attain the lock on all relation before we begin to start dumping data.
        for table in tables:
            position_lock = tables.index(table) + 1
            self.print_progress(
                    position_lock,
                    total_tables,
                    prefix='Acquiring Table Lock (current/total) :',
                    suffix='Done',
                    bar_length=50
            )
            time.sleep(0.1)
            lock_table = self.lock_table_skeleton.format(table)

        for table in tables:
            position_dump = tables.index(table) + 1
            self.print_progress(
                    position_dump,
                    total_tables,
                    prefix='Dumping Table Data (current/total)   :',
                    suffix='Done',
                    bar_length=50
            )
            time.sleep(0.1)
            create, insert = self.__external_tables_activity_builder(table)

        # TODO: Drop the schema once done

    def run_backup(self):
        """
        Run the actual backup
        """

        # Check for all executable and environment before running backup commands
        self.__check_executables()

        # Prepare the database id and folder location where the backup will be stored.
        self.__prepare_backup()

        # Unless explicitly requested not to dump metadata, backup the metadata of objects
        if not self.data_only:
            self.__backup_metadata()

        # Unless explicitly requested not to dump data, dump the data of the objects.
        if not self.schema_only:
            self.__backup_data()

        print self.metadata_backup_dir
        print self.data_backup_dir


HdbBackup(connection_obj).run_backup()