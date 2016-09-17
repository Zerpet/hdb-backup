import datetime, logging
from pgdb import connect, DatabaseError
from lib import check_executables, error_logger, set_connection, run_cmd, print_progress, get_directory, ext_table_sql_generator


class HdbBackup:

    logger = logging.getLogger("hdb_logger")

    def __init__(self):
        """
        Create a HdbBackup object..
        :return
        """
        # Connection parameters
        self.conn = None
        self.cursor = None
        self.username = 'gpadmin'
        self.host = 'localhost'
        self.port = 5432
        self.password = None
        self.dbname = 'postgres'
        self.pxf_port = 51200

        # Backup Parameters
        self.backup_id = None
        self.backup_type = None
        self.table = None
        self.schema = None
        self.exclude_table = None
        self.exclude_schema = None
        self.data_only = False
        self.schema_only = False
        self.clean = False
        self.no_privileges = False
        self.global_dump = False
        self.metadata_backup_dir = None
        self.data_backup_dir = None
        self.force = False
        self.create_database = False
        self.backup_base = "/hawq_backup"
        self.ext_schema_name = 'hawqbackup_schema'

        # Query Skeleton for backup
        self.drop_schema_skeleton = """ DROP SCHEMA IF EXISTS {0} CASCADE """
        self.create_schema_skeleton = """ CREATE SCHEMA {0} """
        self.lock_table_skeleton = """ LOCK TABLE {0} IN ACCESS SHARE MODE """
        self.create_external_table_skeleton = """ CREATE WRITABLE EXTERNAL TABLE {0}.{1} ( like {2} )
                                              LOCATION ('pxf://localhost:{3}{4}/{5}/{6}?profile=HdfsTextSimple')
                                              FORMAT 'TEXT' (DELIMITER = E'\\t') """
        self.insert_external_table_skeleton = """ INSERT INTO {0}.{1} SELECT * FROM {2} """
        self.schema_query_skeleton = """ SELECT COUNT(*) FROM pg_namespace WHERE nspname = '{0}' """

    def set_backup_id(self):
        """Set the backup ID to be used in this backup. The most common ID format is <year><month><day><hour><minute><seconds>
         For example, 20160101170000 will be the standard for January 1st, 2016 at 5 PM.
        :return: Backup ID
        """
        self.logger.debug("Generating the backup ID for the backup")
        self.backup_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        return self.backup_id

    def __backup_metadata(self):
        """
        Backup DDLs and metadata before dumping the data
        :return
        """

        # Source the hawq executable path
        self.logger.info("Source the hawq executable path")
        run_cmd("source $GPHOME/greenplum_path.sh")

        # Backup file name
        ddl_file = self.metadata_backup_dir + '/hdb_dump_' + self.backup_id + '_ddl.dmp'
        global_file = self.metadata_backup_dir + '/hdb_dump_' + self.backup_id + '_global.dmp'

        # Metadata backup command creator
        pg_dump_cmd, pg_dumpall_cmd = self.__get_args(
            "pg_dump",
            "--schema-only",
            "--format=c"
        )
        pg_dump_cmd = ' '.join(pg_dump_cmd)
        pg_dump_cmd += ' | hdfs dfs -put - {0}'.format(ddl_file)
        pg_dump_cmd += ' ; exit $PIPESTATUS;'
        self.logger.info("Executing DDL backup, metadata backup file: \"{0}\"".format(
            ddl_file
        ))
        run_cmd(pg_dump_cmd)

        if pg_dumpall_cmd:
            pg_dumpall_cmd += ' | hdfs dfs -put - {0}'.format(global_file)
            pg_dumpall_cmd += ' ; exit $PIPESTATUS;'
            self.logger.info("Executing global object backup, global backup file: \"{0}\"".format(
                global_file
            ))
            run_cmd(pg_dumpall_cmd)

    def __get_args(self, executable, *args):
        """
        compile all the executable and the arguments, combining with common arguments
        to create a full batch of command args
        :param:
            executable - type of command
            *args      - other Keyword argument parameters
        :return: Argument List for pg_dump, pg_dumpall_cmd command
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

    def __fetch_object_info(self):
        """
        This method is responsible for fetching all the table names (with schema) from a given database
        and based on the option passed it dynamically alters its condition..
        :return: Table data from the database
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
                    AND c.relkind = 'r' and c.relstorage != 'x'
                    AND relname not in (SELECT partitiontablename FROM pg_partitions) """

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

        try:
            self.cursor.execute(query)
        except DatabaseError, e:
            error_logger(e)

        return self.cursor.fetchall()

    def __verify_table_schema(self, tables):
        """
        If the user provided tables / schema then we need to ensure that the user provided list is available in
        the database, if something is missing then we will error out and ask user to fix the list.
        :param tables: list of tables found on the database.
        :return:
        """

        # If user provided a list of tables.
        if self.table:
            self.logger.debug("Checking if the provided list of table is found on the database..")
            user_tables_list = self.table.split(',')
            if len(user_tables_list) != len(tables):
                    error_logger("One or more tables cannot be found on the database \"{0}\","
                                 " provided total table: {1}, found: {2}".format(
                            self.dbname, len(user_tables_list), len(tables)
                    ))

        # If user provided a list of schemas.
        elif self.schema:
            user_schemas_list = self.schema.split(',')
            self.logger.debug("Checking if the provided list of schema is found on the database..")
            for schema in user_schemas_list:
                schema_query = self.schema_query_skeleton.format(schema)
                try:
                    self.cursor.execute(schema_query)
                except DatabaseError, e:
                    error_logger(e)
                result = self.cursor.fetchone()
                if result[0] == 0L:
                    error_logger("Found no schema \"{0}\" in the database \"{1}\"".format(
                        schema, self.dbname
                    ))

    def print_display_info(self):
        """
        This method print all the backup parameters on the screen.
        :return:
        """

        # Define the type of backup
        if self.table:
            self.backup_type = "Selected Table Backup"
        elif self.schema:
            self.backup_type = "Selected Schema Backup"
        elif self.data_only:
            self.backup_type = "Data Only Backup"
        elif self.schema_only:
            self.backup_type = "Schema Only Backup"
        elif self.exclude_schema:
            self.backup_type = "Exclude Schema Backup"
        elif self.exclude_table:
            self.backup_type = "Exclude Table Backup"
        elif not (self.table or self.schema or self.exclude_table or self.exclude_schema or self.schema_only or self.data_only):
            self.backup_type = "Full Backup"

        # Log messages to be printed on the screen.
        self.logger.info("*******************************************************************************************")
        self.logger.info("Database Name: {0}".format(self.dbname))
        self.logger.info("Host Name: {0}".format(self.host))
        self.logger.info("Port Number: {0}".format(self.port))
        self.logger.info("User Name: {0}".format(self.username))
        self.logger.info("Backup Type: {0}".format(self.backup_type))
        self.logger.info("Backup ID: {0}".format(self.backup_id))
        self.logger.info("Backup Table: {0}".format(self.table))
        self.logger.info("Backup Schema: {0}".format(self.schema))
        self.logger.info("Backup Schema Only: {0}".format(self.schema_only))
        self.logger.info("Backup Data Only: {0}".format(self.data_only))
        self.logger.info("Exclude table Name: {0}".format(self.exclude_table))
        self.logger.info("Exclude Schema Name: {0}".format(self.exclude_schema))
        self.logger.info("Include Drop Statement: {0}".format(self.clean))
        self.logger.info("Include Privileges: {0}".format(self.no_privileges))
        self.logger.info("Include Global Dump: {0}".format(self.global_dump))
        self.logger.info("Include Create Database: {0}".format(self.create_database))
        self.logger.info("Metadata Backup Directory: {0}".format(self.metadata_backup_dir))
        self.logger.info("Data Backup Directory: {0}".format(self.data_backup_dir))
        self.logger.info("Force: {0}".format(self.force))
        self.logger.info("External Table Schema Name: {0}".format(self.ext_schema_name))
        self.logger.info("PXF Port: {0}".format(self.pxf_port))
        self.logger.info("*******************************************************************************************")

    def __backup_data(self):
        """
        Backup actual data using external tables. The methods Create External Table and use
        "INSERT INTO ext_table SELECT * from internal_table" to backup the data to HDFS
        :return
        """
        drop_schema = self.drop_schema_skeleton.format(self.ext_schema_name)
        create_schema = self.create_schema_skeleton.format(self.ext_schema_name)

        # In case the previous hawqbackup schema was not cleaned up and user supplied force then drop it
        if self.force:
            self.logger.debug("Attempting to drop the schema \"{0}\"".format(
                self.ext_schema_name
            ))
            self.cursor.execute(drop_schema)

        # Fetch all the tables in the database.
        tables = self.__fetch_object_info()

        # Verify if the tables/schema provided matches the user provided.
        self.__verify_table_schema(tables)

        # Total tables to backup
        total_tables = len(tables)
        self.logger.debug("Total tables to backup is: {0}".format(
            total_tables
        ))

        # Ignore the client message ( like Notice ) on the psql prompt
        self.cursor.execute("set client_min_messages = 'ERROR' ")

        # Create the schema
        try:
            self.logger.debug("Attempting to create the schema: \"{0}\"".format(
                self.ext_schema_name
            ))
            self.cursor.execute(create_schema)
            self.conn.commit()
        except DatabaseError:
            error_logger("Found schema \"{0}\" already exits on the database \"{1}\", "
                         "Try dropping/renaming the schema or use --force option".format(
                    self.ext_schema_name, self.dbname
            ))

        # Loop through the table list to backup the data.
        for table in tables:
            position_dump = tables.index(table) + 1
            print_progress(
                    position_dump,
                    total_tables,
                    prefix='Dumping Table Data (current/total):',
                    suffix='Done',
                    bar_length=50
            )
            create, insert = ext_table_sql_generator(
                self.create_external_table_skeleton,
                self.insert_external_table_skeleton,
                table,
                self.ext_schema_name,
                self.pxf_port,
                self.data_backup_dir
            )
            try:
                self.cursor.execute(create)
                self.cursor.execute(insert)
                self.conn.commit()
            except DatabaseError, e:
                error_logger(e)

        # Drop the schema once done
        try:
            self.logger.debug("Backup is done, drop the schema \"{0}\"".format(
                self.ext_schema_name
            ))
            self.cursor.execute(drop_schema)
            self.conn.close()
        except DatabaseError, e:
            error_logger(e)

    def run_backup(self):
        """
        Run the actual backup
        :return
        """

        # Start time
        self.logger.info("Starting Backup at: {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        # Check for all executable and environment before running backup commands
        self.logger.info("Checking for all the executables that is needed by the program")
        check_executables()

        # Prepare and check connection to the database.
        self.logger.info("Checking the database connectivity")
        self.conn, self.cursor = set_connection(self.dbname, self.host, self.port, self.username, self.password)

        # Set backup id
        self.logger.info("Setting up the database backup ID for this backup")
        self.set_backup_id()

        # Prepare the database id and folder location where the backup will be stored.
        self.logger.info("Preparing all the directories where the backup will be stored")
        self.metadata_backup_dir, self.data_backup_dir = get_directory(self.backup_base, self.backup_id, self.dbname)

        # Display the backup information
        self.print_display_info()

        # Unless explicitly requested not to dump metadata, backup the metadata of objects
        if not self.data_only:
            self.logger.info("Backing up the database DDL.")
            self.__backup_metadata()

        # Unless explicitly requested not to dump data, dump the data of the objects.
        if not self.schema_only:
            self.logger.info("Backing up the data")
            self.__backup_data()

        # End completion message & time
        self.logger.info("Backup of the database \"{0}\" and of the backup type \"{1}\" has completed".format(
            self.dbname, self.backup_type
        ))
        self.logger.info("Backup finished at: {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
