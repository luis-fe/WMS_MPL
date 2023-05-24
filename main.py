from flask import Flask, render_template, jsonify, request
import psycopg2
import os
from apscheduler.schedulers.background import BackgroundScheduler
from functools import wraps
import ConecaoAWSRS
import OPfilaRepor
import Reposicao
import Silk_PesquisaTelas
import Silk_PesquisaNew

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
@app.route('/api/Usuarios/<int:codigo>', methods=['POST'])
@token_required
def update_usuario(codigo):
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()
    # Verifica se a coluna "funcao" está presente nos dados recebidos
    if 'funcao' in data and 'nome' in data:
        # Obtém o valor da coluna "funcao"
        nova_funcao = data['funcao']
        novo_nome = data['nome']
        cursor.execute('UPDATE "Reposicao"."cadusuarios" SET funcao=%s, nome=%s WHERE codigo=%s', (nova_funcao, novo_nome, codigo))
        conn.commit()
        # Retorna uma resposta de sucesso
        return jsonify({'message': f'Funcao e nome do Usuário {codigo} atualizado com sucesso'})
    
    if 'funcao' in data:
        # Obtém o valor da coluna "funcao"
        nova_funcao = data['funcao']
        cursor.execute('UPDATE "Reposicao"."cadusuarios" SET funcao=%s WHERE codigo=%s', (nova_funcao, codigo))
        conn.commit()
        # Retorna uma resposta de sucesso
        return jsonify({'message': f'Funcao do Usuário {codigo} atualizado com sucesso'})
    if 'nome' in data:
        # Obtém o valor da coluna "funcao"
        novo_nome = data['nome']
        cursor.execute('UPDATE "Reposicao"."cadusuarios" SET nome=%s WHERE codigo=%s', (novo_nome, codigo))
        conn.commit()
        # Retorna uma resposta de sucesso
        return jsonify({'message': f'Nome do Usuário {codigo} atualizado com sucesso'})
    # Retorna uma resposta de erro se a coluna "funcao" não estiver presente nos dados
    return jsonify({'message': 'Coluna "funcao" ou "nome" não encontrada nos dados'}), 400

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
                   'count(tr."codbarrastag"), '
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
    
@app.route('/api/DetalhaOP', methods=['GET'])
@token_required
def get_DetalhaOP():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    NumeroOP = request.args.get('numeroOP')
    op = OPfilaRepor.detalhaOP(NumeroOP)
    # Obtém os nomes das colunas
    column_names = op.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in op.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@app.route('/api/DetalhaOPxSKU', methods=['GET'])
@token_required
def get_DetalhaOPxSKU():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    NumeroOP = request.args.get('numeroOP')
    op = OPfilaRepor.detalhaOPxSKU(NumeroOP)
    # Obtém os nomes das colunas
    column_names = op.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in op.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@app.route('/api/Enderecos', methods=['GET'])
@token_required
def get_enderecos():
    enderecos= Reposicao.ObeterEnderecos()
    # Obtém os nomes das colunas
    column_names = enderecos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in enderecos.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)

@app.route('/api/DetalhaEndereco', methods=['GET'])
@token_required
def get_DetalhaEndereco():
    # Obtém o código do endereco e a senha dos parâmetros da URL
    Endereco = request.args.get('Endereco')
    Endereco_det = Reposicao.SituacaoEndereco(Endereco)
    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)
    

@app.route('/api/ApontamentoReposicao', methods=['POST'])
@token_required
def get_ApontaReposicao():
    # Obtenha os dados do corpo da requisição
    data = request.get_json()
    codUsuario = data['codUsuario']
    codbarra = data['codbarra']
    endereco = data['endereco']
    dataHora = data['dataHora']


    #Verifica Se existe atribuicao
    Apontamento = Reposicao.ApontarReposicao(codUsuario,codbarra, endereco, dataHora)
    if Apontamento == False:
        return jsonify({'message': False, 'Status': f'codigoBarras {codbarra} nao existe no Estoque'})
    if Apontamento == 'Reposto':
        return jsonify({'message': False, 'Status': f'codigoBarras {codbarra} ja reposto'})
    else:
        return jsonify({'message': True, 'status':f'Salvo com Sucesso'})
    
    
    
 #ETAPA 2:  Api para acesso do Quadro de Estamparia - Projeto WMS das Telas de  Silk:

@app.route('/api/Silk/PesquisaEndereco', methods=['GET'])
@token_required
def get_PesquisaEndereco():
        Coluna = request.args.get('Coluna')
        Operador = request.args.get('Operador')
        Nome = request.args.get('Nome')

        resultados = Silk_PesquisaTelas.PesquisaEnderecos(Coluna, Operador, Nome)

        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        filaeposicao_data = []
        for row in resultados:
            filaeposicao_dict = {}
            for i, value in enumerate(row):
                filaeposicao_dict[
                    f'col{i + 1}'] = value  # Substitua 'col{i+1}' pelo nome da coluna correspondente, se disponível
            filaeposicao_data.append(filaeposicao_dict)

        return jsonify(filaeposicao_data)
    
@app.route('/api/Silk/deleteTelas', methods=['DELETE'])
@token_required
def delete_endpoint():
    endereco = request.args.get('endereco')
    produto = request.args.get('produto')

    # Chama a função Funcao_Deletar para realizar a exclusão
    resultado = Silk_PesquisaTelas.Funcao_Deletar(endereco, produto)

    if resultado == True:
        return f'endereco: {endereco}, produto {produto}  EXCLUIDOS NO CADASTRO DE SILK', 200
    else:
        return 'Falha ao deletar', 500

@app.route('/api/Silk/IserirTelas', methods=['PUT'])
@token_required
def insert_endpoint():
    produto = request.args.get('produto')
    endereco = request.args.get('endereco')

    # Chama a função Funcao_Inserir para realizar a inserção
    resultado = Silk_PesquisaTelas.Funcao_Inserir(produto, endereco)

    if resultado == True:
        return f'produto{produto} endereço{endereco}, Inserção realizada com sucesso', 200
    else:
        return 'Falha ao inserir', 500
    
@app.route('/api/Silk2/IserirTelas', methods=['PUT'])
@token_required
def insert_endpoint():
    produto = request.args.get('produto')
    endereco = request.args.get('endereco')

    # Chama a função Funcao_Inserir para realizar a inserção
    resultado = Silk_PesquisaNew.Funcao_Inserir(produto, endereco)

    if resultado == True:
        return f'produto{produto} endereço{endereco}, Inserção realizada com sucesso', 200
    else:
        return 'Falha ao inserir', 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
    cursor.close()
    conn.close()
