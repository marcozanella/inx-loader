from flask import Flask, flash, request, redirect, render_template, url_for
from flask_sock import Sock
from werkzeug.utils import secure_filename
from time import sleep
import os, random
from configparser import ConfigParser
import inx.functions

app = Flask(__name__)
socket = Sock(app)

app.secret_key = 'whEtr8uQoB'
app.config['ALLOWED_EXTENSIONS'] = ['xlsx', 'XLSX']
app.config['UPLOAD_FOLDER_INXEU'] = './uploads/inxeu'
app.config['UPLOAD_FOLDER_INXD'] = './uploads/inxd'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1000 * 1000

@socket.route('/echo')
def echo(websocket):
    inx.functions.connect_db(websocket)
    # while True:
    #     data = str(random.random())
    #     websocket.send(inx.functions.connect_db())
    #     inx.functions.use_the_socket(websocket)
    #     # print(data)
    #     sleep(3)

@app.route("/")
def home():
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
def digital():
    clear_folders(app.config['UPLOAD_FOLDER_INXD'])
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
        location = app.config['UPLOAD_FOLDER_INXD']
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
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

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


if __name__ == "__main__":
    app.run()