import datetime, logging, sys
from pgdb import connect, DatabaseError
from lib import check_executables, error_logger, set_connection, run_cmd, print_progress, get_directory, ext_table_sql_generator, confirm


class HDBRestore:

    logger = logging.getLogger("hdb_logger")

    def __init__(self):
        """
        Create HdbRestore object..
        """
        # Connection parameters
        self.conn = None
        self.cursor = None
        self.username = 'gpadmin'
        self.host = 'localhost'
        self.port = 5432
        self.password = None
        self.to_dbname = None
        self.from_dbname = 'postgres'
        self.pxf_port = 51200

        # Restore Parameters
        self.backup_id = None
        self.force = False
        self.metadata_backup_dir = None
        self.data_backup_dir = None
        self.data_only = False
        self.schema_only = False
        self.restore_type = None
        self.generate_list = None
        self.user_list = None
        self.clean = False
        self.create_target_db = False
        self.target_db_encoding = 'UTF8'
        self.global_restore = False
        self.no_privileges = False
        self.ignore = False
        self.no_prompt = False
        self.restore_base = "/hawq_backup"
        self.ext_schema_name = 'hawqrestore_schema'
        self.generate_list_location = '/tmp/backup_list_' + self.backup_id

        # Query Skeleton for backup
        self.drop_schema_skeleton = """ DROP SCHEMA IF EXISTS {0} CASCADE """
        self.create_schema_skeleton = """ CREATE SCHEMA {0} """
        self.create_external_table_skeleton = """ CREATE EXTERNAL TABLE {0}.{1} ( like {2} )
                                              LOCATION ('pxf://localhost:{3}{4}/{5}/{6}?profile=HdfsTextSimple')
                                              FORMAT 'TEXT' (DELIMITER = E'\\t') """
        self.insert_external_table_skeleton = """ INSERT INTO {2} SELECT * FROM {0}.{1} """

    def __restore_metadata(self):
        """
        Restore the metadata and global objects is its a full restore
        :return:
        """
        # Source the hawq executable path
        self.logger.info("Source the hawq executable path")
        run_cmd("source $GPHOME/greenplum_path.sh")

        # Backup file name
        ddl_file = self.metadata_backup_dir + '/hdb_dump_' + self.backup_id + '_ddl.dmp'
        global_file = self.metadata_backup_dir + '/hdb_dump_' + self.backup_id + '_global.dmp'

        # If request to create database then create it with default encoding if not provided.
        if self.create_target_db:
            create_db_cmd = 'createdb ' + self.to_dbname + ' -E ' + self.target_db_encoding
            run_cmd(create_db_cmd)

        # Metadata restore command creator
        pg_restore_cmd = self.__get_args(
                "pg_restore",
                "--schema-only"
        )
        pg_restore_cmd = ' '.join(pg_restore_cmd)
        metadata_file = 'hdfs dfs -cat ' + ddl_file + ' | '

        # If generate list is requested.
        if self.generate_list:
            pg_restore_cmd += ' > ' + self.generate_list_location
            pg_restore_cmd = metadata_file + pg_restore_cmd + ' ; exit $PIPESTATUS;'
            run_cmd(pg_restore_cmd, self.ignore)
            self.logger.info("Backup List for the backup ID \"{0}\" is generated at location: \"{1}\"".format(
                self.backup_id, self.generate_list_location
            ))
            sys.exit(0)

        # Else then this a full restore or user list restore
        else:
            pg_restore_cmd = metadata_file + pg_restore_cmd + ' ; exit $PIPESTATUS;'
            run_cmd(pg_restore_cmd, self.ignore)

        # If full restore or if requested to restore the global dump then
        if self.global_restore or not (self.generate_list or self.user_list):
            read_hdfs_global_file = 'hdfs dfs -cat ' + global_file + ' | '
            psql_cmd = 'psql -d ' + self.to_dbname + ' -U ' + self.username
            global_restore_cmd = read_hdfs_global_file + psql_cmd
            run_cmd(global_restore_cmd)

    def __get_args(self, executable, *args):
        """
        compile all the executable and the arguments, combining with common arguments
        to create a full batch of command args
        :return:
        """
        args = list(args)
        args.insert(0, executable)

        # If drop schema needed
        if self.clean:
            args.append(
                    "--clean"
            )

        # If Grant/Revoke not needed
        if self.no_privileges:
            args.append(
                    "--no-privileges"
            )

        # If generate list is needed for the backup ID
        if self.generate_list:
            args.append(
                    "--list"
            )

        # If User list is provided
        if self.user_list:
            args.append(
                "--use-list={0}".format(self.user_list)
            )

        # If database name
        if self.to_dbname:
            args.append(
                "--dbname={0}".format(self.to_dbname)
            )
        else:
            error_logger("No Database Name specified onto where it needs to restore")

        return args

    def __get_data_location(self):
        """
        Get all the schema and table names that this directory holds the backup for
        :return: list of all relation that it has the backup
        """

        cmd = "hdfs dfs -ls " + self.data_backup_dir + '/*'
        output = run_cmd(cmd)
        backup_object_list = []
        for directory in output.split('\n'):

            # Ignore blanks space and other unwanted output
            if (directory.startswith('Found') and directory.endswith('items')) or not directory.strip():
                pass
            # For the rest get the table and schema names
            else:
                schema = '"' + directory.split('/')[-2] + '"'
                table = '"' + directory.split('/')[-1] + '"'
                backup_object_list.append(
                       schema + '.' + table
                )

        return backup_object_list

    def __read_user_list(self):
        """
        Read the file of user provided list to restore and obtain only the line that has
        TABLE in its method.
        :return:
        """
        contents = [line.rstrip('\n') for line in open(self.user_list)]
        user_provided_restore_list = []
        for content in contents:
            if 'TABLE' in content and not content.startswith(';'):
                table = '"' + content.split()[-2] + '"'
                schema = '"' + content.split()[-3] + '"'
                user_provided_restore_list.append(
                    schema + '.' + table
                )
        return user_provided_restore_list

    def __restore_data(self):
        """
        This methods restore the data found in the backup key directory
        It first created a readable external table and then use it to read the content
        and then store it in the database tables.
        :return:
        """
        drop_schema = self.drop_schema_skeleton.format(self.ext_schema_name)
        create_schema = self.create_schema_skeleton.format(self.ext_schema_name)

        # In case the previous hawqrestore schema was not cleaned up and user supplied force then drop it
        if self.force:
            self.logger.debug("Attempting to drop the schema \"{0}\"".format(
                self.ext_schema_name
            ))
            self.cursor.execute(drop_schema)
            self.conn.commit()

        # Get the list of relations that this backup ID has the backup for.
        relation_list = self.__get_data_location()

        # Get the user provided restore list
        if self.user_list:
            user_restore_tables = self.__read_user_list()
            for table in relation_list:
                if table not in user_restore_tables:
                    self.logger.debug("Removing the relation {0} from the data restore list, "
                                      "since its not part of user provided restore list".format(table))
                    relation_list.remove(table)

        # Total tables to restore
        total_tables = len(relation_list)
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
                            self.ext_schema_name, self.to_dbname))

        # Loop through the list to restore
        for table in relation_list:

            # print table, relation_list
            position_dump = relation_list.index(table) + 1
            print_progress(
                    position_dump,
                    total_tables,
                    prefix='Restoring Table Data (current/total):',
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
            self.conn.commit()
            self.conn.close()
        except DatabaseError, e:
            error_logger(e)

    def print_display_info(self):
        """
        This prints all the restore parameters on the screen or on the logs
        :return:
        """
        # Define the type of restore
        if self.generate_list:
            self.restore_type = "Generate the backup list"
        elif self.user_list:
            self.restore_type = "Restoring User List"
        elif self.data_only:
            self.restore_type = "Data Only Restore"
        elif self.schema_only:
            self.restore_type = "Schema Only Restore"
        elif not (self.schema_only or self.generate_list or self.user_list or self.data_only):
            self.restore_type = "Full Restore"

        # Log messages to be printed on the screen.
        self.logger.info("*******************************************************************************************")
        self.logger.info("Source Database Name: {0}".format(self.from_dbname))
        self.logger.info("Target Database Name: {0}".format(self.to_dbname))
        self.logger.info("Host Name: {0}".format(self.host))
        self.logger.info("Port Number: {0}".format(self.port))
        self.logger.info("User Name: {0}".format(self.username))
        self.logger.info("Create Target Database: {0}".format(self.create_target_db))
        self.logger.info("Target Database Encoding: {0}".format(self.target_db_encoding))
        self.logger.info("Restore Type: {0}".format(self.restore_type))
        self.logger.info("Backup ID: {0}".format(self.backup_id))
        self.logger.info("Restore Global Restore: {0}".format(self.global_restore))
        self.logger.info("Restore Schema Only: {0}".format(self.schema_only))
        self.logger.info("Restore Data Only: {0}".format(self.data_only))
        self.logger.info("Drop Objects before restore: {0}".format(self.clean))
        self.logger.info("Ignore Privileges: {0}".format(self.no_privileges))
        self.logger.info("Ignore Errors: {0}".format(self.ignore))
        self.logger.info("User Provided list to restore: {0}".format(self.user_list))
        self.logger.info("Metadata Restore Directory: {0}".format(self.metadata_backup_dir))
        self.logger.info("Data Restore Directory: {0}".format(self.data_backup_dir))
        self.logger.info("Force: {0}".format(self.force))
        self.logger.info("External Table Schema Name: {0}".format(self.ext_schema_name))
        self.logger.info("PXF Port: {0}".format(self.pxf_port))
        self.logger.info("*******************************************************************************************")

        # Ask for confirmation
        if not self.no_prompt:
            choice = confirm("Is the above restore parameters correct and do you wish to continue")
            if choice == 'no':
                self.logger.info("Aborting due to user request....")
                sys.exit(0)

    def run_restore(self):
        """
        Run the restore steps.
        :return:
        """

        # Start time
        self.logger.info("Starting Restore at: {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        # Check for all executable and environment before running backup commands
        self.logger.info("Checking for all the executables that is needed by the program")
        check_executables()

        # Check if backup key is provided
        if not self.backup_id:
            error_logger("No backup key specified, restore can't continue")

        # If no to_dbname is not given then make to_dbname = from_dbname
        if not self.to_dbname:
            self.logger.info("This database is going to restore the data to the database \"{0}\"".format(
                self.from_dbname
            ))
            self.to_dbname = self.from_dbname

        # Prepare and check connection to the database.
        self.logger.info("Checking the database connectivity")
        self.conn, self.cursor = set_connection(self.to_dbname, self.host, self.port, self.username, self.password)

        # Prepare the folder and get location where the backup is stored.
        self.logger.info("Preparing to get all the directories where the backup is stored")
        self.metadata_backup_dir, self.data_backup_dir = get_directory(self.restore_base, self.backup_id, self.from_dbname)

        # Display the restore information
        self.print_display_info()

        # Unless explicitly requested not to restore metadata, restore the metadata of objects
        if not self.data_only:
            self.logger.info("Restoring the DDL")
            self.__restore_metadata()

        # Unless explicitly requested not to restore data, restore the data of the objects.
        if not self.schema_only:
            self.logger.info("Restoring the data")
            self.__restore_data()

        # End completion message & time
        self.logger.info("Restore of the database \"{0}\" and of the restore type \"{1}\" has completed".format(
            self.to_dbname, self.restore_type
        ))
        self.logger.info("Restore finished at: {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))