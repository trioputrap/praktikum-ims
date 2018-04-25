import time
import helper
import config

HISTORY_1 = config.FILE_1
HISTORY_2 = config.FILE_2

TB_NAME = "invoice_sync"

DB_1 = helper.connect(config.DB_1)
DB_2 = helper.connect(config.DB_2)

QUERY_SELECT = "SELECT * FROM invoice_sync"

while(True):
    CUR_1 = DB_1.cursor()
    CUR_2 = DB_2.cursor()

    CUR_1.execute(QUERY_SELECT)

    results = CUR_1.fetchall()

    history = helper.read_data(HISTORY_1)
    results = list(map(lambda x: tuple(map(str,x)),results))

    is_modified = False
    i = 0
    helper.sync_indexes.clear()
    for row in results:
        #found
        index = helper.find_by_id(history, row[0])
        if(index != -1):
            #modified
            if(row != history[index]):
                helper.print_timestamp("row " + str(i) + " : modified")
                print("row main\t: " + str(row))
                print("row history\t: " + str(history[index]))
                history[index] = row

                query = helper.query_update_builder(TB_NAME, row)
                helper.print_timestamp("EXECUTE QUERY : " + query)
                # lakukan query update ke DB_2
                CUR_2.execute(query);
                is_modified = True

        #not found == insert
        else:
            query = helper.query_insert_builder(TB_NAME, row)
            helper.print_timestamp("EXECUTE QUERY : " + query)
            CUR_2.execute(query)

            history.append(row)
            index = helper.find_by_id(history, row[0])
            is_modified = True

        helper.sync_indexes.append(index)
        i += 1

    #delete
    if(len(helper.sync_indexes) < len(history)):
        i = 0
        while(i<len(history)):
            if(not helper.sync_indexes.__contains__(i)):
                query = helper.query_delete_builder(TB_NAME, history[i])
                helper.print_timestamp("EXECUTE QUERY : " + query)
                CUR_2.execute(query)
            i+=1

        is_modified = True


    if (is_modified):
        helper.save_data(HISTORY_1, results)
        helper.save_data(HISTORY_2, results)
        helper.print_timestamp(HISTORY_1 +" & "+ HISTORY_2 +" has beed updated!")
    else:
        helper.print_timestamp("nothing changed")

    #commit and close cursor
    is_modified = False

    DB_1.commit()
    DB_2.commit()

    CUR_1.close()
    CUR_2.close()

    time.sleep(config.DELAY)