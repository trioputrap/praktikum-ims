import time
import helper
import config
import _thread


class Integration:
    db1_config = []
    db2_config = []
    tables = []
    db1_connection = None
    db2_connection = None
    sync_indexes = []

    def __init__(self, db1_config, db2_config, tables):
        self.db1_config = db1_config
        self.db2_config = db2_config
        self.tables = tables
        self.db1_connection = helper.connect(db1_config)
        self.db2_connection = helper.connect(db2_config)
        # create table backups

        cur_1 = helper.get_cursor(self.db1_connection)
        cur_2 = helper.get_cursor(self.db2_connection)
        for table in self.tables:
            query = "CREATE TABLE IF NOT EXISTS `%s_history` SELECT * FROM `%s` WHERE 1=0" % (table,table)
            cur_1.execute(query)
            cur_2.execute(query)
        cur_1.close()
        cur_2.close()

    def sync(self, tb_name):
        while (True):
            print(tb_name)
            db_1 = helper.connect(self.db1_config)
            db_2 = helper.connect(self.db2_config)
            CUR_1 = helper.get_cursor(db_1)
            CUR_2 = helper.get_cursor(db_2)

            CUR_1.execute("SELECT * FROM " + tb_name)
            results = list(CUR_1.fetchall())
            CUR_1.execute("SELECT * FROM `" + tb_name + "_history`")
            history = list(CUR_1.fetchall())
            print(results)
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
                        # lakukan query update ke DB_2
                        CUR_1.execute(query_history);
                        CUR_2.execute(query);
                        CUR_2.execute(query_history);
                        is_modified = True

                # not found == insert
                else:
                    query = helper.query_insert_builder(tb_name, row)
                    query_history = helper.query_insert_builder(tb_name+"_history", row)
                    helper.print_timestamp("EXECUTE QUERY : " + query)
                    CUR_1.execute(query_history)
                    CUR_2.execute(query)
                    CUR_2.execute(query_history)

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
                        CUR_1.execute(query_history)
                        CUR_2.execute(query)
                        CUR_2.execute(query_history)
                    i += 1

                is_modified = True

            if (is_modified):
                #helper.save_data(tb_name+"_history", results, CUR_1)
                #helper.save_data(tb_name+"_history", results, CUR_2)
                helper.print_timestamp(tb_name + " & " + tb_name + "_history has beed updated!")
            else:
                helper.print_timestamp("nothing changed")

            # commit and close cursor
            is_modified = False

            db_1.commit()
            db_2.commit()
            
            CUR_1.close()
            CUR_2.close()
            time.sleep(config.DELAY)

    def run(self):
        for table in self.tables:
            _thread.start_new_thread(self.sync, (table,))