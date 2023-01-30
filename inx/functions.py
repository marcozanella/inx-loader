from configparser import ConfigParser
import pyodbc, random
import glob
import pandas as pd

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

def run_process(conx, files, ws):
    #print(files)
    #ws.send(files)
    files_dict = {}
    duties ={}
    for file in files:
        if "ke30" in file:
            duties["ke30"] = file
        if "ke24" in file:
            duties["ke24"] = file
        if "zaq" in file:
            duties["zaq"] = file
        if "oo" in file:
            duties["oo"] = file
        if "oh" in file:
            duties["oh"] = file
        if "oit" in file:
            duties["oit"] = file
        if "arr" in file:
            duties["arr"] = file
        if "prl" in file:
            duties["prl"] = file
    ws.send("duties: " + str(duties))
    # ws.send("Reading file, it may take some time ..., don't refresh or leave the page ")
    # df = pd.read_excel(duties["ke30"])
    # # ws.send(files_dict["ke30"] + " - " + str(len(df)) + "records ")
    # ws.send(str(len(df)) + "records ")

    for duty_key, duty in duties.items():
        ws.send("duty_key: " + duty_key + ", duty: " + duty)
        
def grind_the_file():
    pass