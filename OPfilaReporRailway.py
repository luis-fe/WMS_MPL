import ConexaoPostgreRailway
import pandas as pd
import numpy

def ProdutividadeRepositores():
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('select tr."Usuario", '
                   'count(tr."codbarrastag"), '
                   'substring(tr."DataReposicao",1,10) as DataReposicao '
                   'from "Reposicao"."tagsreposicao" tr '
                   'group by "Usuario" ,substring("DataReposicao",1,10)')
    TagReposicao = cursor.fetchall()
    return TagReposicao

def FilaPorOP():
    conn = ConexaoPostgreRailway.conexao()
    df_OP1 = pd.read_sql(' select "numeroop", "totalop" as qtdpeçs_total, "Usuario" as codusuario_atribuido, count("numeroop") as qtdpeçs_arepor  from "Reposicao"."filareposicaoportag" frt ' 
                        '  group by "numeroop", "Usuario", "totalop"  ',conn)

    df_OP_Iniciada =pd.read_sql(' select "numeroop", count("numeroop") as qtdpeçs_reposto  from "Reposicao"."tagsreposicao" frt ' 
                        ' group by "numeroop" ',conn)
    df_OP1 = pd.merge(df_OP1,df_OP_Iniciada,on='numeroop',how='left')
    usuarios = pd.read_sql(
        'select codigo as codusuario_atribuido , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ', conn)
    usuarios['codusuario_atribuido'] = usuarios['codusuario_atribuido'].astype(str)
    df_OP1 = pd.merge(df_OP1, usuarios, on='codusuario_atribuido', how='left')
    df_OP1['qtdpeçs_reposto'] = df_OP1['qtdpeçs_reposto'].replace('', numpy.nan).fillna('0')
    df_OP1['qtdpeçs_total'] = df_OP1['qtdpeçs_total'].replace('', numpy.nan).fillna('0')
    df_OP1['qtdpeçs_arepor'] = df_OP1['qtdpeçs_arepor'].replace('', numpy.nan).fillna('0')
    df_OP1['qtdpeçs_total'] = df_OP1['qtdpeçs_total'].astype(int)
    df_OP1 = df_OP1[df_OP1['qtdpeçs_total']>0]
    df_OP1['qtdpeçs_reposto'] = df_OP1['qtdpeçs_reposto'].astype(int)
    df_OP1['% Reposto'] = 1 - numpy.divide(df_OP1['qtdpeçs_arepor'],df_OP1['qtdpeçs_total'])
    df_OP1['% Reposto'] = df_OP1['% Reposto'] * 100
    df_OP1['% Reposto'] = df_OP1['% Reposto'].round(2)
    
    # Clasificando o Dataframe para analise
    df_OP1 = df_OP1.sort_values(by='qtdpeçs_total', ascending=False, ignore_index=True)  # escolher como deseja classificar
    df_OP1["Situacao"] = df_OP1.apply(lambda row: 'Iniciada'  if row['qtdpeçs_reposto'] >0 else 'Nao Iniciada', axis=1)
    df_OP1['codusuario_atribuido'] = df_OP1['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    df_OP1['nomeusuario_atribuido'] = df_OP1['nomeusuario_atribuido'].replace('', numpy.nan).fillna('-')
    conn.close()
    # Limitar o número de linhas usando head()
    df_OP1 = df_OP1.head(50)  # Retorna as 3 primeiras linhas
    return df_OP1

def detalhaOP(numeroop):
    conn = ConexaoPostgreRailway.conexao()
    df_op = pd.read_sql('select "numeroop" , "codbarrastag", "epc", "Usuario" as codusuario_atribuido, "Situacao", "codReduzido" '
                   'from "Reposicao"."filareposicaoportag" frt where "numeroop" = ' +"'"+  numeroop +"'", conn)

    df_op.rename(columns={'codbarrastag': 'codbarrastag_1'}, inplace=True)
    df_op['codusuario_atribuido'] = df_op['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    df_op2 = pd.read_sql(
        'select "numeroop" , "codbarrastag" AS codbarrastag2   '
        'from "Reposicao"."tagsreposicao" frt where "numeroop" = ' + "'" + numeroop + "'", conn)

    df_op = pd.merge(df_op, df_op2, on='numeroop', how='left')
    df_op['codbarrastag2'] = df_op['codbarrastag2'].replace('', numpy.nan).fillna('-')
    df_op['codbarrastag'] = df_op.apply(lambda row: row['codbarrastag2']  if row['codbarrastag2'] != '-' else row['codbarrastag_1'], axis=1)
    df_op['SituacaoTag'] = df_op.apply(
        lambda row: 'bipada' if row['codbarrastag2'] != '-' else 'Não Iniciada', axis=1)
    # Remover coluna 'B'
    df_op = df_op.drop('codbarrastag2', axis=1)
    df_op = df_op.drop('codbarrastag_1', axis=1)
    usuarios = pd.read_sql('select codigo as codusuario_atribuido , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ',conn)
    usuarios['codusuario_atribuido'] = usuarios['codusuario_atribuido'].astype(str)
    df_op = pd.merge(df_op,usuarios,on='codusuario_atribuido',how='left')
    df_op['codusuario_atribuido'] = df_op['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    df_op['nomeusuario_atribuido'] = df_op['nomeusuario_atribuido'].replace('', numpy.nan).fillna('-')
    conn.close()
    df_op.to_csv('verficiar.csv')
    if df_op.empty:
        return pd.DataFrame({'Status': [False],'Mensagem':['OP nao Encontrada']})
    else:
        return  df_op

def ConsultaSeExisteAtribuicao(numeroop):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('select "numeroop", "Usuario"  from "Reposicao"."filareposicaoportag" frt  '
                   'WHERE "numeroop" = %s AND "Usuario" IS NULL', (numeroop,))
    # Obter o número de linhas afetadas
    NumeroLInhas = cursor.rowcount

    cursor.close()
    conn.close()
    return NumeroLInhas

def AtribuiRepositorOP(codigo, numeroop):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('update "Reposicao"."filareposicaoportag" '
                   'set "Usuario"  = %s where "numeroop" = %s'
                   , (codigo, numeroop))
    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    
def detalhaOPxSKU(numeroop):
    conn = ConexaoPostgreRailway.conexao()
    df_op = pd.read_sql('select "numeroop", "codReduzido", "CodEngenharia", "Cor", "tamanho", "descricao" '
                   'from "Reposicao"."filareposicaoportag" frt where "numeroop" = ' +"'"+  numeroop +"'"+
                   'group by "numeroop", "codReduzido","descricao" , "Cor","tamanho","CodEngenharia"', conn)

    conn.close()
    if df_op.empty:
        return pd.DataFrame({'Status': [False],'Mensagem':['OP nao Encontrada']})
    else:
        return  df_op

