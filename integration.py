import time
import helper
import config
import _thread
import json
from datetime import datetime


class Integration:
    db_master_config = []
    db_slave_config = []
    tables = ()

    master_sync = []
    master_slave_sync = []

    sync_indexes = []
    history_postfix = "_history"

    def connect_to_master(self):
        try:
            connection = helper.connect(self.db_master_config)
            #helper.print_timestamp("connection to master : SUCCESS!")
            return connection
        except:
            helper.print_timestamp("connection to master : failed")
        return None

    def connect_to_slave(self):
        try:
            connection = helper.connect(self.db_slave_config)
            #helper.print_timestamp("connection to slave : SUCCESS!")
            return connection
        except:
            helper.print_timestamp("connection to slave : failed")
        return None

    def execute(self, cursor, sql):
        try:
            cursor.execute(sql)
            return True
        except:
            helper.print_timestamp("error in execution : " + sql)
            return False

    def create_table_history(self, db):
        cursor = helper.get_cursor(db)
        for table in self.tables:
            query = "CREATE TABLE IF NOT EXISTS `%s%s` SELECT * FROM `%s` WHERE 1=0" % (
            table['name'], self.history_postfix, table['name'])
            try:
                cursor.execute(query)
            except:
                helper.print_timestamp("problem in creating table history")
        cursor.close()

    def get_backup_name(self, table_name):
        return self.db_master_config['host'] + "_" + table_name + ".json"

    def read_backup(self, table, db):

        table_history = {"name": table['name']+self.history_postfix, "id": table['id']}
        cursor = helper.get_cursor(db)

        file_backup = self.db_master_config['host'] + "_" + self.db_master_config['db_name'] + "_backup.json"
        last_backup = dict(helper.read_json(file_backup))

        last_backup_table = '' if len(last_backup) == 0 else last_backup[table['name']]

        file = self.db_slave_config['host'] + "_" + table['name'] + ".json"
        backup_data = helper.read_json(file)

        is_backup = False
        if (len(backup_data)>0):
            last_row = {}
            for row in backup_data:
                if (row['datetime']>last_backup_table):
                    is_backup = True
                    if(row['dml_mode']==1):
                        query = helper.query_insert_builder(table['name'], row['data'])
                        query_history = helper.query_insert_builder(table_history['name'], row['data'])
                    elif (row['dml_mode']==2):
                        query = helper.query_update_builder(table, row['data'], row['id'])
                        query_history = helper.query_update_builder(table_history, row['data'], row['id'])
                    else:
                        query = helper.query_delete_builder(table, row['data']['id'])
                        query_history = helper.query_delete_builder(table_history, row['data']['id'])

                    last_row = row
                    print("last")
                    print(last_row)
                    print(query)
                    print(self.db_master_config)
                    if(self.execute(cursor, query)):
                        if(self.execute(cursor, query_history)):
                            print("backup sukses")
                            db

            if(is_backup):
                last_backup[table['name']] = last_row['datetime']
                try:
                    f = open(self.db_master_config['host'] + "_" + self.db_master_config['db_name'] + "_backup.json", "w")
                    data = json.dumps(last_backup)
                    f.write(data)
                    f.close()
                except:
                    print("fail file")
        db.commit()
        cursor.close()



    def __init__(self, db_master_config, db_slave_config, tables):
        self.db_master_config = db_master_config
        self.db_slave_config = db_slave_config
        self.tables = tables

    def sync(self, table):
        table_history = {"name": table['name']+self.history_postfix, "id": table['id']}
        while (True):
            db_master = self.connect_to_master()
            self.master_sync.clear()
            self.master_slave_sync.clear()

            if(db_master is None):
                helper.print_timestamp("retrying connect to master...")
            else :
                self.create_table_history(db_master)
                cur_master = helper.get_cursor(db_master)

                #connect if
                db_slave = self.connect_to_slave()
                cur_slave = None
                try:
                    cur_slave = helper.get_cursor(db_slave)
                    self.create_table_history(db_slave)
                except:
                    helper.print_timestamp("error in slave connection")

                cur_master.execute("SELECT * FROM `%s`" % table['name'])
                results = list(cur_master.fetchall())

                cur_master.execute("SELECT * FROM `%s%s`" % (table['name'], self.history_postfix))
                history = list(cur_master.fetchall())

                is_modified = False
                i = 0
                self.sync_indexes.clear()
                for row in results:
                    # found
                    index = helper.find_by_id(history, row[table['id']])
                    if (index != -1):
                        # modified
                        if (row != history[index]):
                            helper.print_timestamp("row " + str(i) + " : modified")
                            print("row main\t: " + str(row))
                            print("row history\t: " + str(history[index]))

                            row_update = helper.get_modified_col(row, history[index])

                            history[index] = row

                            query = helper.query_update_builder(table, row_update, row[table['id']])
                            query_history = helper.query_update_builder(table_history, row_update, row[table['id']])
                            helper.print_timestamp("EXECUTE QUERY : " + query)

                            # lakukan query update ke db_slave
                            if(self.execute(cur_master, query_history)):
                                if(self.execute(cur_slave, query)):
                                    if(self.execute(cur_slave, query_history)): self.master_slave_sync.append(row)
                                else:
                                    self.master_sync.append(row)
                                    row_backup = helper.get_row_json(row_update, "UPDATE", row[table['id']])
                                    print(row_backup)
                                    helper.save_json(self.get_backup_name(table['name']), row_backup)

                            is_modified = True

                    # not found == insert
                    else:
                        query = helper.query_insert_builder(table['name'], row)
                        query_history = helper.query_insert_builder(table['name']+"_history", row)
                        helper.print_timestamp("EXECUTE QUERY : " + query)

                        if(self.execute(cur_master, query_history)):
                            if (self.execute(cur_slave, query)):
                                if (self.execute(cur_slave, query_history)): self.master_slave_sync.append(row)
                            else:
                                self.master_sync.append(row)
                                row_backup = helper.get_row_json(row, "INSERT")
                                print(row_backup)
                                helper.save_json(self.get_backup_name(table['name']), row_backup)

                        history.append(row)
                        index = helper.find_by_id(history, row['id'])
                        is_modified = True

                    self.sync_indexes.append(index)
                    i += 1

                # delete
                if (len(self.sync_indexes) < len(history)):
                    i = 0
                    while (i < len(history)):
                        if (not self.sync_indexes.__contains__(i)):
                            query = helper.query_delete_builder(table, history[i][table['id']])
                            query_history = helper.query_delete_builder(table_history, history[i][table['id']])
                            helper.print_timestamp("EXECUTE QUERY : " + query)

                            if(self.execute(cur_master, query_history)):
                                if(self.execute(cur_slave, query)):
                                    if(self.execute(cur_slave, query_history)): self.master_slave_sync.append(history[i])
                                else:
                                    self.master_sync.append(history[i])
                                    row_backup = helper.get_row_json(history[i], "DELETE")
                                    print(row_backup)
                                    print(self.get_backup_name(table['name']))
                                    helper.save_json(self.get_backup_name(table['name']), row_backup)

                        i += 1

                    is_modified = True

                if (is_modified):
                    helper.print_timestamp("synchronized : \n"+str(len(self.master_sync))+" local \n"+str(len(self.master_slave_sync))+" master-slave")
                else:
                    helper.print_timestamp(table['name'] + " : nothing changed")

                # commit and close cursor

                if (not db_master is None):
                    db_master.commit()

                if (not db_slave is None):
                    db_slave.commit()

                if (not cur_master is None):
                    cur_master.close()

                if (not cur_slave is None):
                    cur_slave.close()

                self.read_backup(table, db_master)
            time.sleep(5)

    def run(self):
        for table in self.tables:
            _thread.start_new_thread(self.sync, (table,))