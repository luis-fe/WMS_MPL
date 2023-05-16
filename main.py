from flask import Flask, render_template, jsonify
import psycopg2
import os
from apscheduler.schedulers.background import BackgroundScheduler

import RecarregarBanco



app = Flask(__name__)

port = int(os.environ.get('PORT', 5000))

db_name = "wms_bd"
db_user = "wms"
db_password = "Master100"
db_host = "wmsbd.cyiuowfro4wv.sa-east-1.rds.amazonaws.com"
portbanco = "5432"



def my_task():
    # coloque o código que você deseja executar continuamente aqui
    RecarregarBanco.FilaTags()
    print('Executando tarefa...')
    
scheduler = BackgroundScheduler()
scheduler.add_job(func=my_task, trigger='interval', seconds=270)
scheduler.start()
    
conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port= portbanco)
cursor = conn.cursor()



@app.route('/')
def home():
    return render_template('index.html')


@app.route('/api/Filaeposicao', methods=['GET'])
def get_usuarios():
    cursor.execute('select * from "Reposicao"."FilaReposicaoTags" frt ')
    Filaeposicao = cursor.fetchall()
    return jsonify(Filaeposicao)

@app.route('/api/Usuarios', methods=['GET'])
def get_usuarios():
    cursor.execute('select * from "Reposicao"."cadusuarios" c  ')
    Filaeposicao = cursor.fetchall()
    return jsonify(Filaeposicao)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
    cursor.close()
    conn.close()
