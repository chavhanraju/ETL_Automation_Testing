import mysql.connector
import cx_Oracle

class DB_Conn():
    def MySQL_DB_Conn(self):
        Source_db_conn=mysql.connector.connect(user='root',password="admin",database="source",host="localhost")
        #print("Source: MySQL db connection Successfully")
        return Source_db_conn

    def Oracle_DB_Conn(self):
        Taeget_DB_conn = cx_Oracle.connect(user="c##target",password="target",dsn="localhost:1521/xe")
        #print("Target: Oracle db connection Successfully")
        return Taeget_DB_conn

#db_cls=DB_Conn()
#db_cls.MySQL_DB_Conn()
#db_cls.Oracle_DB_Conn()



