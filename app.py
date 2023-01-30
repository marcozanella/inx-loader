from flask import Flask, flash, request, redirect, render_template, render_template_string, url_for, session
from flask_sock import Sock
from werkzeug.utils import secure_filename
from time import sleep
import os, glob
from configparser import ConfigParser
import inx.functions

app = Flask(__name__)
socket = Sock(app)
app.secret_key = 'whEtr8uQoB'


@socket.route('/echo')
def echo(websocket):
    # get_config(session["db"])
    print( "session[\"db\"] ", session["db"])
    result = inx.functions.connect_db(websocket, config_dict["connection_string"])
    if result != False:
        conx = result[1] #From the tuple returned by the connection function, in the inx.function
        # Here we need to work on the files
        # Fare un elenco dei file che ci sono nella cartella uploads - tuple?
        files = glob.glob(config_dict["upload_folder"] + "/*")
        inx.functions.run_process(conx, files, websocket)
        
        
        # tipo nomefile
        # lavorare i files

        
@app.route("/")
def home():
    # Set ConfigParser and read ini file
    return render_template("home.html")

@app.route('/inxeu', methods=['POST', 'GET'])
def inxeu():
    clear_folders(app.config['UPLOAD_FOLDER_INXEU'])
    if request.method == 'POST':
        if 'form_ke30' in request.files and 'form_ke24' in request.files:
            the_ke30_fileobject = request.files['form_ke30']
            the_ke24_fileobject = request.files['form_ke24']
        if the_ke30_fileobject.filename == '' and the_ke24_fileobject.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if the_ke30_fileobject.filename != '':
            if the_ke30_fileobject and allowed_file(the_ke30_fileobject.filename):
                the_ke30_filename = securify_filename(the_ke30_fileobject.filename)
                the_ke30_fileobject.save(os.path.join(app.config['UPLOAD_FOLDER_INXEU'], the_ke30_filename))
        if the_ke24_fileobject.filename != '':
            if the_ke24_fileobject and allowed_file(the_ke24_fileobject.filename):
                the_ke24_filename = securify_filename(the_ke24_fileobject.filename)
                the_ke24_fileobject.save(os.path.join(app.config['UPLOAD_FOLDER_INXEU'], the_ke24_filename))
        return render_template('result.html', request=request)
    elif request.method == 'GET':
        return render_template('inxeu.html', request=request)

@app.route('/inxd', methods=['POST', 'GET'])
def inxd():
    # Getting the endpoint name
    # endpoint = request.url_rule.endpoint
    session["db"] = request.url_rule.endpoint
    print ( session["db"] )
    get_config(session["db"])
    clear_folders(config_dict["upload_folder"])
    if request.method == 'POST':
        if 'form_ke30' in request.files and 'form_ke24' in request.files:
            the_ke30_fileobject = request.files['form_ke30']
            the_ke24_fileobject = request.files['form_ke24']
            the_zaq_fileobject = request.files['form_zaq']
            the_oo_fileobject = request.files['form_oo']
            the_oh_fileobject = request.files['form_oh']
            the_oit_fileoject = request.files['form_oit']
            the_arr_fileoject = request.files['form_arr']
            the_prl_fileoject = request.files['form_prl']
        if the_ke30_fileobject.filename == '' and the_ke24_fileobject.filename == '' and the_zaq_fileobject.filename == '' and the_oo_fileobject.filename == '' and the_oh_fileobject.filename == '' and the_oit_fileoject.filename == '' and the_arr_fileoject.filename == '' and the_prl_fileoject.filename == '':
            flash('No selected file')
            return redirect(request.url)
        location = config_dict["upload_folder"]
        check_filename_and_saves_on_server(the_ke30_fileobject, location, 'ke30')
        check_filename_and_saves_on_server(the_ke24_fileobject, location, 'ke24')
        check_filename_and_saves_on_server(the_zaq_fileobject, location, 'zaq')
        check_filename_and_saves_on_server(the_oo_fileobject, location, 'oo')
        check_filename_and_saves_on_server(the_oh_fileobject, location, 'oh')
        check_filename_and_saves_on_server(the_oit_fileoject, location, 'oit')
        check_filename_and_saves_on_server(the_arr_fileoject, location, 'arr')
        check_filename_and_saves_on_server(the_prl_fileoject, location, 'prl')
        # return redirect(url_for("logger"))
        return render_template('logger.html', request=request)
    if request.method == 'GET':
        # return render_template('base.html', request=request)
        return render_template('inxd.html', request=request)

@app.route("/logger")
def logger():
    return render_template("logger.html")

@app.route('/settings')
def settings():
    config_file = ConfigParser()
    config_file.read("config.ini")
    sections = config_file.sections()
    for section in sections:
        print (section)
        keys = config_file[section]
        for key in keys:
            print("\t", key, "\t\t", config_file[section][key])
    return render_template("settings.html")

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in config_dict["allowed_extensions"]

def securify_filename(filename):
    return secure_filename(filename)

def check_filename_and_saves_on_server(fileobject, location_to_save, filetype):
    if fileobject.filename != '':
        if fileobject and allowed_file(fileobject.filename):
            filename = securify_filename(fileobject.filename)
            # fileobject.save(url_for('uploads', filename=fileobject.filename))
            fileobject.save(os.path.join(location_to_save, filetype + "__" + filename))

def clear_folders(folder):
    folder += '/'
    for path, subdirs, files in os.walk(folder):
        for name in files:
            if os.path.isfile(os.path.join(path,name)):
                os.remove(os.path.join(path,name))

def process_file(sock, data):
    sleep(1)
    sock.send("from process_file" + data)

def get_config(company):
    # Get these setting from settings.ini
    config = ConfigParser()
    config.read('config.ini')
    global config_dict
    config_dict = {
        "sql_driver_ver": config["DEFAULT"]["sql_driver_ver"],
        "max_content_length": config["DEFAULT"]["max_content_length"],
        "allowed_extensions": config["DEFAULT"]["allowed_extensions"]
    }
    if company == "inxd":
        config_dict["db_server"] = config["INXD_SERVER_CONFIG"]["inxd_db_host"]
        config_dict["db_name"] = config["INXD_SERVER_CONFIG"]["inxd_db_name"]
        config_dict["db_username"] = config["INXD_SERVER_CONFIG"]["inxd_db_username"]
        config_dict["db_password"] = config["INXD_SERVER_CONFIG"]["inxd_db_password"]
        config_dict["upload_folder"] = config["INXD_SERVER_CONFIG"]["inxd_upload_folder"]
    else:
        config_dict["db_server"] = config["INXEU_SERVER_CONFIG"]["inxeu_db_host"]
        config_dict["db_name"] = config["INXEU_SERVER_CONFIG"]["inxeu_db_name"]
        config_dict["db_username"] = config["INXEU_SERVER_CONFIG"]["inxeu_db_username"]
        config_dict["db_password"] = config["INXEU_SERVER_CONFIG"]["inxeu_db_password"]
        config_dict["upload_folder"] = config["INXEU_SERVER_CONFIG"]["inxeu_upload_folder"]
    connection_string = "DRIVER={ODBC Driver " + config_dict["sql_driver_ver"] + " for SQL Server};SERVER=" + config_dict["db_server"] + ";DATABASE=" + config_dict["db_name"] + ";UID=" + config_dict["db_username"] + ";PWD=" + config_dict["db_password"] + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    config_dict["connection_string"] = connection_string

if __name__ == "__main__":
    app.run()