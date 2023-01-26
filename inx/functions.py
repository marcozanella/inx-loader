from configparser import ConfigParser
import pyodbc

def connect_db():
    config = ConfigParser()
    config.read("config.ini")
    sql_drv_ver = config["DEFAULT"]["sql_driver_ver"]
    db_server = config["INXD_SERVER_CONFIG"]["inxd_db_host"]
    db_name = config["INXD_SERVER_CONFIG"]["inxd_db_name"]
    db_user = config["INXD_SERVER_CONFIG"]["inxd_db_username"]
    db_password = config["INXD_SERVER_CONFIG"]["inxd_db_password"]
    conn_string = "DRIVER={ODBC Driver " + str(sql_drv_ver) + " for SQL Server};SERVER=" + db_server + ";DATABASE=" + db_name + ";UID=" + db_user + ";PWD=" + db_password + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return conn_string
    # socket.send("connecting to db ...")
    # conx = pyodbc.connect(conn_string)
