from configparser import ConfigParser
import pyodbc, random

def connect_db_inxd():
    config = ConfigParser()
    config.read("config.ini")
    sql_drv_ver = config["DEFAULT"]["sql_driver_ver"]
    db_server = config["INXD_SERVER_CONFIG"]["inxd_db_host"]
    db_name = config["INXD_SERVER_CONFIG"]["inxd_db_name"]
    db_user = config["INXD_SERVER_CONFIG"]["inxd_db_username"]
    db_password = config["INXD_SERVER_CONFIG"]["inxd_db_password"]
    conn_string = "DRIVER={ODBC Driver " + str(sql_drv_ver) + " for SQL Server};SERVER=" + str(db_server) + ";DATABASE=" + db_name + ";UID=" + db_user + ";PWD=" + db_password + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return conn_string

def connect_db_inxeu():
    config = ConfigParser()
    config.read("config.ini")
    sql_drv_ver = config["DEFAULT"]["sql_driver_ver"]
    db_server = config["INXD_SERVER_CONFIG"]["inxeu_db_host"]
    db_name = config["INXD_SERVER_CONFIG"]["inxeu_db_name"]
    db_user = config["INXD_SERVER_CONFIG"]["inxeu_db_username"]
    db_password = config["INXD_SERVER_CONFIG"]["inxeu_db_password"]
    conn_string = "DRIVER={ODBC Driver " + str(sql_drv_ver) + " for SQL Server};SERVER=" + db_server + ";DATABASE=" + db_name + ";UID=" + db_user + ";PWD=" + db_password + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return conn_string

def connect_db(the_socket, db):
    if db == "inxd":
        conn_string = connect_db_inxd()
    else:
        conn_string = connect_db_inxeu()

    the_socket.send("Connection String:")
    the_socket.send(conn_string)
    try:
        conx = pyodbc.connect(conn_string)
        the_socket.send("Connection established successfully")
        # the_socket.send(conx)
        # HURRAY!
        return (True, conx)
    except pyodbc.ConnectionError as ex:
        the_socket.send("connection Error")
        return False
    except pyodbc.OperationalError as err:
        the_socket.send("DB connection could not be established")
        return False
    except pyodbc.ProgrammingError as ex:
        the_socket.send (ex.args[1])
        return False
    except Exception as ex:
        the_socket.send("Database connection terminated with unhandled eception")
        the_socket.send(ex)
        return False

def use_the_socket(ws):
    ws.send(random.random())
