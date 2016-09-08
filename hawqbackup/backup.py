import psycopg2
from hawqlogger import LOG


class HdbBackup:

    """Create a HdbBackup object with a connection object
    :param connection_obj An initialized Psycopg2 connection object
    """
    def __init__(self, connection_obj):
        assert type(connection_obj) == psycopg2.extensions.connection
        self.conn = connection_obj
        self.backup_id = None

    """Set the backup ID to be used in this backup. The most common ID format is <year><month><day><hour><minute>
     For example, 201601011700 will be the standard for January 1st, 2016 at 5 PM.
     :param backup_id backup ID to be used in this backup
    """
    def set_backup_id(self, backup_id):
        assert backup_id == int
        self.backup_id = backup_id

    """Set a new connection object for this backup
    :param connection_obj is a Psycopg2 connection object
    """
    def set_connection(self, connection_obj):
        assert type(connection_obj) == psycopg2.extensions.connection
        self.conn = connection_obj

    """This method is responsible for creating all the external tables used to dump the data from the internal tables
    """
    def __create_external_tables(self, tables):
        pass

    """This method is responsible for fetching all the table names (with schema) from a given database
    :param database is the name of the target database"""
    def __fetch_tables(self):
        pass

    """This method is responsible for fetching all the tables in a given schema list. Expect _schema_ param to be a list
    or a tuple"""
    def __fetch_tables_in_schema(self, schema):
        pass

    """Prepare HDFS folders to backup the given database"""
    def __prepare_backup(self):
        pass

    """Backup DDLs and metadata before dumping the data"""
    def __backup_metadata(self):
        pass

    """Backup actual data into external tables. This method is to run the
    INSERT INTO ext_table SELECT * from internal_table"""
    def __backup_data(self):
        pass

    """Run the actual backup"""
    def run_backup(self):
        self.__prepare_backup()
        self.__backup_metadata()
        self.__backup_data()
        pass
