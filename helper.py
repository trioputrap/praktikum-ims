import pymysql
import datetime

sync_indexes = []

def save_data(file, data):
    f = open(file, "w")
    for row in data:
        f.write(','.join(map(str, row)))
        f.write('\n')
    f.close()

def read_data(file):
    data = []
    f = open(file, "r")
    for line in f:
        line = line.replace("\n", "")
        data.append(tuple(line.split(',')))
    f.close()

    return data

def find_by_id(data, id):
    i = 0
    for row in data:
        if (row[0] == id):
            return i
        i+=1
    return -1

def query_update_builder(tb_name, data):
    sql = "UPDATE " + tb_name
    sql += " SET `invoice_id` = '" + data[1] + "',"
    sql += " `total_invoice` = '" + data[2] + "',"
    sql += " `invoice_status` = '" + data[3] + "',"
    sql += " `paid_at` = '" + data[4] + "'"
    sql += " WHERE `id` = '" + data[0] + "'"
    return sql

def query_insert_builder(tb_name, data):
    sql = "INSERT INTO " + tb_name
    sql += " VALUES('"+data[0]+"', '"+data[1]+"', '"+data[2]+"', '"+data[3]+"', '"+data[4]+"')"
    return sql

def query_delete_builder(tb_name, data):
    sql = "DELETE FROM " + tb_name
    sql += " WHERE `id` = '"+data[0]+"'"
    return sql

def connect(db_config):
    return pymysql.connect(
        db_config['host'],
        db_config['user'],
        db_config['pass'],
        db_config['db_name']
    )

def print_timestamp(msg):
    print("\n[" + str(datetime.datetime.now()) + "]")
    print("> " + msg)