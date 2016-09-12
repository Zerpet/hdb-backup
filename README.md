HDB Backup and Restore utility
==============================

This tool aims to help automating HDB backups and restore tasks. This
tool will be capable of parallel backup and restore a database to/from 
HDFS.

# Requirements
- Python 2.6+
- Psycopg2  (python module)
- argparse  (python module)
- hdfs3     (python module)
- HDB 2.x
- PXF

Most of the testing will be done under HDP 2 platform. It _should_ work
under other platforms. Feel free to test and provide feedback :)
