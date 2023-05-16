from flask import Flask, render_template, jsonify
import psycopg2
import os
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

port = int(os.environ.get('PORT', 5000))

db_name = "wms_bd"
db_user = "wms"
db_password = "Master100"
db_host = "wmsbd.cyiuowfro4wv.sa-east-1.rds.amazonaws.com"
portbanco = "5432"

conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)
cursor = conn.cursor()

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/Filaeposicao', methods=['GET'])
def get_filaeposicao():
    cursor.execute('select * from "Reposicao"."FilaReposicaoTags" frt ')
    filaeposicao = cursor.fetchall()
    return jsonify(filaeposicao)

@app.route('/api/Usuarios', methods=['GET'])
def get_usuarios():
    cursor.execute('select * from "Reposicao"."cadusuarios" c  ')
    usuarios = cursor.fetchall()
    return jsonify(usuarios)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
    cursor.close()
    conn.close()
