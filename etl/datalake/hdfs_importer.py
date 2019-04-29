import subprocess
import json
from copy import deepcopy

class CannotImportException(Exception):
    pass

class Importer(object):
    conf = {}
    # to make sure of connection strings below and add other connections like teradata
    db_connection_strings = {
        "oracle": "jdbc:oracle:thin:@",
        "mysql": "jdbc:mysql"
    }

    def __init__(self):
        def _get_configuration(conf_file):
            with open(self.configuration_file) as f:
                return json.load(f)
        self.conf = _get_configuration("importer.json")

    @classmethod
    def exec_cmd(cls, cmd):
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True)
        proc.communicate()
        return_code = proc.returncode
        return return_code

    def import_to_hdfs(self, query_import=False):
        _conf = deepcopy(self.conf)
        host = _conf['metadata']['host']
        port = _conf['metadata']['port']
        schema = _conf['metadata']['schema']
        user = _conf['metadata']['user']
        password = _conf['metadata']['password']
        query = _conf['metadata']['query']
        target_dir = _conf['metadata']['target_dir']
        db = _conf['metadata']['db_type']
        if query_import:
            cmd = ['sqoop', 'import', '--connect', '%s:@//%s:%d/%s' % (db, host, port, schema) ,
               '--username', user ,'--password', password,
               '--query', '"%s"' % query, '-m', '1',
               '--target-dir', target_dir]
        else:
            # TO DO
            # sqoop command from table
            cmd = ['sqoop', 'import', '--connect', '%s:@//%s:%d/%s' % (db, host, port, schema),
                   '--username', user, '--password', password,
                   #'--query', '"%s"' % query, '-m', '1',
                   '--target-dir', target_dir]
        return_code = self.exec_cmd(cmd)
        if return_code:
            raise ('Cannot load data in hdfs from oracle sql query .')



