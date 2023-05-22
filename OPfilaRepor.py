import pandas as pd
import ConecaoAWSRS
import numpy

def FilaPorOP():
    conn = ConecaoAWSRS.conexao()
    df_OP1 = pd.read_sql(' select "numeroop", count("numeroop")as QtdPeçs_total, "Usuario" as codusuario_atribuido  from "Reposicao"."FilaReposicaoporTag" frt ' 
                        '  group by "numeroop", "Usuario"  ',conn)
    df_OP_Iniciada =pd.read_sql(' select "numeroop", count("numeroop") as QtdPeçs_Reposto  from "Reposicao"."FilaReposicaoporTag" frt ' 
                        '  where "Situacao" = '+ "'nao iniciada'"+ ' group by "numeroop" ',conn)
    df_OP1 = pd.merge(df_OP1,df_OP_Iniciada,on='numeroop',how='left')
    usuarios = pd.read_sql(
        'select codigo as codusuario_atribuido , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ', conn)
    usuarios['codusuario_atribuido'] = usuarios['codusuario_atribuido'].astype(str)
    df_OP1 = pd.merge(df_OP1, usuarios, on='codusuario_atribuido', how='left')
    df_OP1['qtdpeçs_reposto'] = df_OP1['qtdpeçs_reposto'].replace('', numpy.nan).fillna('0')
    df_OP1['qtdpeçs_total'] = df_OP1['qtdpeçs_total'].astype(int)
    df_OP1['qtdpeçs_reposto'] = df_OP1['qtdpeçs_reposto'].astype(int)
    df_OP1['% Reposto'] = numpy.divide(df_OP1['qtdpeçs_reposto'],df_OP1['qtdpeçs_total'])
    # Clasificando o Dataframe para analise
    df_OP1 = df_OP1.sort_values(by='qtdpeçs_total', ascending=False, ignore_index=True)  # escolher como deseja classificar
    df_OP1["Situacao"] = df_OP1.apply(lambda row: 'Iniciada'  if row['qtdpeçs_reposto'] >0 else 'Nao Iniciada', axis=1)
    df_OP1['codusuario_atribuido'] = df_OP1['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    df_OP1['nomeusuario_atribuido'] = df_OP1['nomeusuario_atribuido'].replace('', numpy.nan).fillna('-')
    conn.close()
    # Limitar o número de linhas usando head()
    df_OP1 = df_OP1.head(50)  # Retorna as 3 primeiras linhas
    return df_OP1

def AtribuiRepositorOP(codigo, numeroop):
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    cursor.execute('update "Reposicao"."FilaReposicaoporTag" '
                   'set "Usuario"  = %s where "numeroop" = %s'
                   , (codigo, numeroop))
    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    return  numero_linhas_afetadas

def ConsultaSeExisteAtribuicao(numeroop):
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    cursor.execute('select "numeroOp", "Usuario"  from "Reposicao"."FilaReposicaoporTag" frt  '
                   'WHERE "numeroOp" = %s AND "Usuario" IS NULL', (numeroop,))
    # Obter o número de linhas afetadas
    NumeroLInhas = cursor.rowcount

    cursor.close()
    conn.close()
    return NumeroLInhas

def detalhaOP(numeroop):
    conn = ConecaoAWSRS.conexao()
    df_op = pd.read_sql('select "numeroOp" , "codBarrasTag" , epc, "Usuario" as codusuario_atribuido, "Situacao" '
                   'from "Reposicao"."FilaReposicaoporTag" frt where "numeroOp" = ' +"'"+  numeroop +"'", conn)
    df_op['codusuario_atribuido'] = df_op['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    usuarios = pd.read_sql('select codigo as codusuario_atribuido , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ',conn)
    usuarios['codusuario_atribuido'] = usuarios['codusuario_atribuido'].astype(str)
    df_op = pd.merge(df_op,usuarios,on='codusuario_atribuido',how='left')
    df_op['codusuario_atribuido'] = df_op['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    df_op['nomeusuario_atribuido'] = df_op['nomeusuario_atribuido'].replace('', numpy.nan).fillna('-')
    conn.close()
    if df_op.empty:
        return pd.DataFrame({'Status': [False],'Mensagem':['OP nao Encontrada']})
    else:
        return  df_op
   

  
