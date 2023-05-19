
from flask import Flask, render_template, jsonify, request
import psycopg2
import os
from apscheduler.schedulers.background import BackgroundScheduler
from functools import wraps
import ConecaoAWSRS
import OPfilaRepor

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

# Definindo  conexao com o banco de dados nativo
conn = ConecaoAWSRS.conexao()
cursor = conn.cursor()

# Decorator para verificar o token fixo
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401
    return decorated_function

# Rota pagina inicial
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/FilaReposicao', methods=['GET'])
@token_required
def get_filaeposicao():
    cursor.execute('select * from "Reposicao"."FilaReposicaoporTag" frt limit 10000 ')
    filaeposicao = cursor.fetchall()
    # Obtém os nomes das colunas
    column_names = [desc[0] for desc in cursor.description]
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    filaeposicao_data = []
    for row in filaeposicao:
        filaeposicao_dict = {}
        for i, value in enumerate(row):
            filaeposicao_dict[column_names[i]] = value
        filaeposicao_data.append(filaeposicao_dict)
    return jsonify(filaeposicao_data)


# Rota protegida que requer o token fixo para trazer os Usuarios Cadastrados
@app.route('/api/Usuarios', methods=['GET'])
@token_required
def get_usuarios():
    cursor.execute('select codigo, nome, funcao, situacao from "Reposicao"."cadusuarios" c')
    usuarios = cursor.fetchall()
    # Obtém os nomes das colunas
    column_names = [desc[0] for desc in cursor.description]
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = {}
        for i, value in enumerate(row):
            usuario_dict[column_names[i]] = value
        usuarios_data.append(usuario_dict)
    return jsonify(usuarios_data)


# Rota para atualizar um usuário pelo código
@app.route('/api/Usuarios/<int:codigo>', methods=['PUT'])
@token_required
def update_usuario(codigo):
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()
    # Verifica se a coluna "funcao" está presente nos dados recebidos
    if 'funcao' in data:
        # Obtém o valor da coluna "funcao"
        nova_funcao = data['funcao']
        cursor.execute('UPDATE "Reposicao"."cadusuarios" SET funcao=%s WHERE codigo=%s', (nova_funcao, codigo))
        conn.commit()
        # Retorna uma resposta de sucesso
        return jsonify({'message': 'Usuário atualizado com sucesso'})
    # Retorna uma resposta de erro se a coluna "funcao" não estiver presente nos dados
    return jsonify({'message': 'Coluna "funcao" não encontrada nos dados'}), 400

@app.route('/api/Usuarios', methods=['POST'])
@token_required
def criar_usuario():
    # Obtenha os dados do corpo da requisição
    novo_usuario = request.get_json()
    # Extraia os valores dos campos do novo usuário
    codigo = novo_usuario.get('codigo')
    funcao = novo_usuario.get('funcao')
    nome = novo_usuario.get('nome')
    senha = novo_usuario.get('senha')
    situacao = novo_usuario.get('situacao')
    # Faça a lógica para inserir o novo usuário no banco de dados
    # (substitua com sua lógica de banco de dados)
    # Exemplo: Inserindo o novo usuário em uma tabela "Usuarios"
    # usando SQL
    cursor.execute(
        'INSERT INTO "Reposicao"."cadusuarios" (codigo, funcao, nome, senha, situacao) '
        'VALUES (%s, %s, %s, %s, %s)',
        (codigo, funcao, nome, senha, situacao)
    )
    conn.commit()
    # Retorne uma resposta indicando o sucesso da operação
    return jsonify({'message': 'Novo usuário criado com sucesso'}), 201

#Rota com parametros para check do Usuario e Senha
@app.route('/api/UsuarioSenha', methods=['GET'])
@token_required
def check_user_password():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codigo = request.args.get('codigo')
    senha = request.args.get('senha')

    # Verifica se o código do usuário e a senha foram fornecidos
    if codigo is None or senha is None:
        return jsonify({'message': 'Código do usuário e senha devem ser fornecidos.'}), 400

    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    query = 'SELECT COUNT(*) FROM "Reposicao"."cadusuarios" WHERE codigo = %s AND senha = %s'
    cursor.execute(query, (codigo, senha))
    result = cursor.fetchone()[0]
    
    # Verifica se o usuário existe
    if result == 1:
        # Consulta no banco de dados para obter informações adicionais do usuário
        cursor.execute('SELECT  nome, funcao, situacao FROM "Reposicao"."cadusuarios" WHERE codigo = %s', (codigo,))
        user_data = cursor.fetchone()

        # Verifica se foram encontradas informações adicionais do usuário
        if user_data:
            nome, funcao, situacao = user_data

            # Retorna as informações adicionais do usuário
            return jsonify({
                "status": True,
                "message": "Usuário e senha VALIDADOS!",
                "nome": nome,
                "funcao": funcao,
                "situacao": situacao
            })
        else:
            return jsonify({'message': 'Não foi possível obter informações adicionais do usuário.'}), 500
    else:
        return jsonify({"status": False,
            "message":'Usuário ou senha não existe'}), 401
    
@app.route('/api/TagsReposicao/Resumo', methods=['GET'])
@token_required
def get_TagsReposicao():
    cursor.execute('select tr."Usuario", '
                   'count(tr."Codigo_Barras"), '
                   'substring(tr."DataReposicao",1,10) as DataReposicao '
                   'from "Reposicao"."TagsReposicao" tr '
                   'group by "Usuario" ,substring("DataReposicao",1,10)')
    TagReposicao = cursor.fetchall()
    # Obtém os nomes das colunas
    column_names = [desc[0] for desc in cursor.description]
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    TagReposicao_data = []
    for row in TagReposicao:
        TagReposicao_dict = {}
        for i, value in enumerate(row):
            TagReposicao_dict[column_names[i]] = value
        TagReposicao_data.append(TagReposicao_dict)
    return jsonify(TagReposicao_data)

@app.route('/api/FilaReposicaoOP', methods=['GET'])
@token_required
def get_FilaReposicaoOP():
    FilaReposicaoOP = OPfilaRepor.FilaPorOP()
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    FilaReposicaoOP_data = []
    for index, row in FilaReposicaoOP.iterrows():
        FilaReposicaoOP_dict = {}
        for column_name in column_names:
            FilaReposicaoOP_dict[column_name] = row[column_name]
        FilaReposicaoOP_data.append(FilaReposicaoOP_dict)
    return jsonify(FilaReposicaoOP_data)

@app.route('/api/AtribuirOPRepositor', methods=['POST'])
@token_required
def get_AtribuirOPRepositor():
    # Obtenha os dados do corpo da requisição
    data = request.get_json()
    OP = data['numeroOP']
    Usuario = data['codigo']
    #Verifica Se existe atribuicao
    existe = OPfilaRepor.ConsultaSeExisteAtribuicao(OP)
    if existe == 0:
        # Retorna uma resposta de existencia
        return jsonify({'message': f'OP já foi Atribuida'})
    else:
        OPfilaRepor.AtribuiRepositorOP(Usuario,OP)
        # Retorna uma resposta de sucesso
        return jsonify({'message': True})
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
    cursor.close()
    conn.close()
