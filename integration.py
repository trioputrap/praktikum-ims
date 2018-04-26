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

    def __init__(self, db1_config, db2_config, tables):
        self.db1_config = db1_config
        self.db2_config = db2_config
        self.tables = tables
        self.db1_connection = helper.connect(db1_config)
        self.db2_connection = helper.connect(db2_config)
        # create files
        for table in self.tables:
            try:
                file = open(self.get_history1_name(table), 'r')
            except IOError:
                file = open(self.get_history1_name(table), 'w')
            file.close()
            try:
                file = open(self.get_history2_name(table), 'r')
            except IOError:
                file = open(self.get_history2_name(table), 'w')
            file.close()

    def get_history1_name(self, tb_name):
        return self.db1_config['host'] + "." + self.db1_config['db_name'] + "." + tb_name + ".dat"

    def get_history2_name(self, tb_name):
        return self.db2_config['host'] + "." + self.db2_config['db_name'] + "." + tb_name + ".dat"

    def sync(self, tb_name):
        while (True):
            print(tb_name)
            db_1 = helper.connect(self.db1_config)
            db_2 = helper.connect(self.db2_config)
            CUR_1 = helper.get_cursor(db_1)
            CUR_2 = helper.get_cursor(db_2)

            CUR_1.execute("SELECT * FROM " + tb_name)

            results = CUR_1.fetchall()
            history = helper.read_data(self.get_history1_name(tb_name))

            is_modified = False
            i = 0
            helper.sync_indexes.clear()
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
                        helper.print_timestamp("EXECUTE QUERY : " + query)
                        # lakukan query update ke DB_2
                        CUR_2.execute(query);
                        is_modified = True

                # not found == insert
                else:
                    query = helper.query_insert_builder(tb_name, row)
                    helper.print_timestamp("EXECUTE QUERY : " + query)
                    CUR_2.execute(query)

                    history.append(row)
                    index = helper.find_by_id(history, row['id'])
                    is_modified = True

                helper.sync_indexes.append(index)
                i += 1

            # delete
            if (len(helper.sync_indexes) < len(history)):
                i = 0
                while (i < len(history)):
                    if (not helper.sync_indexes.__contains__(i)):
                        query = helper.query_delete_builder(tb_name, history[i])
                        helper.print_timestamp("EXECUTE QUERY : " + query)
                        CUR_2.execute(query)
                    i += 1

                is_modified = True

            if (is_modified):
                helper.save_data(self.get_history1_name(tb_name), results)
                helper.save_data(self.get_history2_name(tb_name), results)
                helper.print_timestamp(
                    self.get_history1_name(tb_name) + " & " + self.get_history2_name(tb_name) + " has beed updated!")
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