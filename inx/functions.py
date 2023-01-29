from configparser import ConfigParser
import pyodbc, random
import glob

def connect_db(the_socket, conn_string):
    
    the_socket.send("Connection String:")
    the_socket.send(conn_string)
    try:
        conx = pyodbc.connect(conn_string)
        the_socket.send("Connection established successfully")
        # the_socket.send(conx)
        # HURRAY!
        return (True, conx)
    except pyodbc.ConnectionError as ex:
        the_socket.send("Connection Error")
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


def prep_file():
    files_dict = {
        "ke30": "",
        "ke24": "",
        "zaq": "",
        "oo": "",
        "oh": "",
        "oit": "",
        "arr": "",
        "prl": "",
    }
    files = glob.glob('./upload/inxd/*.*')
    pass

