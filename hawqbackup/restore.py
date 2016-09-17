import datetime, logging
from pgdb import connect, DatabaseError
from lib import check_executables, error_logger, set_connection, run_cmd, print_progress, get_directory


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
        self.dbname = 'postgres'
        self.pxf_port = 51200

        # Restore Parameters
        self.backup_id = None
        self.force = False
        self.metadata_backup_dir = None
        self.data_backup_dir = None
        self.restore_base = "/hawq_backup"

    def __restore_metadata(self):
        """
        Restore the metadata and global objects is its a full restore
        :return:
        """
        pass

    def __get_args(self):
        """
        compile all the executable and the arguments, combining with common arguments
        to create a full batch of command args
        :return:
        """
        pass

    def __external_table_activity_bulider(self):
        """
        This method is responsible for creating all the external tables used to restore the data from the internal tables
        @:param: table - table name (i.e in the format schema-name.table-name)
        :return:
        """
        pass

    def __restore_data(self):
        """
        This methods restore the data found in the backup key directory
        It first created a readable external table and then use it to read the content
        and then store it in the database tables.
        :return:
        """
        pass

    def print_display_info(self):
        """
        This prints all the restore parameters on the screen or on the logs
        :return:
        """
        pass

    def run_restore(self):
        """
        Run the restore steps.
        :return:
        """
        pass