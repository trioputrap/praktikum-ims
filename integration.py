import time
import helper
import config
import _thread


class Integration:
    db_master_config = []
    db_slave_config = []
    tables = []

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
            helper.print_timestamp("connection to master : FAILED!")
        return None

    def connect_to_slave(self):
        try:
            connection = helper.connect(self.db_slave_config)
            #helper.print_timestamp("connection to slave : SUCCESS!")
            return connection
        except:
            helper.print_timestamp("connection to slave : FAILED!")
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
            table, self.history_postfix, table)
            try:
                cursor.execute(query)
            except:
                helper.print_timestamp("problem in creating table history")
        cursor.close()

    def __init__(self, db_master_config, db_slave_config, tables):
        self.db_master_config = db_master_config
        self.db_slave_config = db_slave_config
        self.tables = tables

    def sync(self, tb_name):
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

                cur_master.execute("SELECT * FROM `%s`" % tb_name)
                results = list(cur_master.fetchall())

                cur_master.execute("SELECT * FROM `%s%s`" % (tb_name, self.history_postfix))
                history = list(cur_master.fetchall())

                is_modified = False
                i = 0
                self.sync_indexes.clear()
                for row in results:
                    # found
                    index = helper.find_by_id(history, row['id'])
                    if (index != -1):
                        # modified
                        if (row != history[index]):
                            helper.print_timestamp("row " + str(i) + " : modified")
                            print("row main\t: " + str(row))
                            print("row history\t: " + str(history[index]))
                            history[index] = row

                            query = helper.query_update_builder(tb_name, row)
                            query_history = helper.query_update_builder(tb_name+"_history", row)
                            helper.print_timestamp("EXECUTE QUERY : " + query)

                            # lakukan query update ke db_slave
                            self.execute(cur_master, query_history)

                            if(self.execute(cur_slave, query)):
                                if(self.execute(cur_slave, query_history)): self.master_slave_sync.append(row)
                            else:
                                self.master_sync.append(row)

                            is_modified = True

                    # not found == insert
                    else:
                        query = helper.query_insert_builder(tb_name, row)
                        query_history = helper.query_insert_builder(tb_name+"_history", row)
                        helper.print_timestamp("EXECUTE QUERY : " + query)

                        self.execute(cur_master, query_history)

                        if (self.execute(cur_slave, query)):
                            if (self.execute(cur_slave, query_history)): self.master_slave_sync.append(row)
                        else:
                            self.master_sync.append(row)

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
                            query = helper.query_delete_builder(tb_name, history[i])
                            query_history = helper.query_delete_builder(tb_name+"_history", history[i])
                            helper.print_timestamp("EXECUTE QUERY : " + query)

                            self.execute(cur_master, query_history)

                            if(self.execute(cur_slave, query)):
                                if(self.execute(cur_slave, query_history)): self.master_slave_sync.append(history[i])
                            else:
                                self.master_sync.append(history[i])

                        i += 1

                    is_modified = True

                if (is_modified):
                    helper.print_timestamp("synchronized : \n"+str(len(self.master_sync))+" local \n"+str(len(self.master_slave_sync))+" master-slave")
                else:
                    helper.print_timestamp(tb_name + " : nothing changed")

                # commit and close cursor

                if (not db_master is None):
                    db_master.commit()

                if (not db_slave is None):
                    db_slave.commit()

                if (not cur_master is None):
                    cur_master.close()

                if (not cur_slave is None):
                    cur_slave.close()

            time.sleep(1)

    def run(self):
        for table in self.tables:
            _thread.start_new_thread(self.sync, (table,))