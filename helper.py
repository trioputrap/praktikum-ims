import pymysql
import datetime
import json
import config

sync_indexes = []

def save_data(file, data):
    f = open(file, "w")
    f.write(json.dumps(data))
    f.close()

def read_data(file):
    f = open(file, "r")
    try:
        data = json.loads(f.read())
    except:
        data = []
    f.close()

    return data

def find_by_id(data, id):
    i = 0
    for row in data:
        if (row['id'] == id):
            return i
        i+=1
    return -1

def query_update_builder(tb_name, data):
    sql = "UPDATE " + tb_name + " SET"
    first = True
    for field, value in data.items():
        sql+="," if not first else ""
        sql+="`"+field+"`="
        sql += "NULL" if value is None else "'" + str(value) + "'"
        first = False
    field_id = tuple(data)[config.ID_TB_INDEX]
    sql += " WHERE `"+ field_id +"` = '" + str(data.get(field_id)) + "'"
    return sql

def query_insert_builder(tb_name, data):
    sql = "INSERT INTO " + tb_name +"("
    first=True
    for field in data:
        sql+="," if not first else ""
        sql+=field
        first=False
    sql+=") VALUES("
    first = True
    for value in data.values():
        sql+="," if not first else ""
        sql += "NULL" if value is None else "'" + str(value) +"'"
        first=False
    sql += ")"
    return sql

def query_delete_builder(tb_name, data):
    sql = "DELETE FROM " + tb_name
    field_id = tuple(data)[config.ID_TB_INDEX]
    sql += " WHERE `"+ field_id +"` = '" + str(data.get(field_id)) + "'"
    return sql

def connect(db_config):
    return pymysql.connect(
        db_config['host'],
        db_config['user'],
        db_config['pass'],
        db_config['db_name']
    )

def get_cursor(db):
    return db.cursor(pymysql.cursors.DictCursor)

def print_timestamp(msg):
    print("\n[" + str(datetime.datetime.now()) + "]")
    print("> " + msg)