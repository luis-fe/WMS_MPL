import pandas as pd
import ConecaoAWSRS
import numpy

def FilaPorOP():
    conn = ConecaoAWSRS.conexao()
    df_OP1 = pd.read_sql(' select "numeroOp", count("numeroOp")as QtdPeçs_total  from "Reposicao"."FilaReposicaoporTag" frt ' 
                        '  group by "numeroOp" ',conn)
    df_OP_Iniciada =pd.read_sql(' select "numeroOp", count("numeroOp") as QtdPeçs_Reposto  from "Reposicao"."FilaReposicaoporTag" frt ' 
                        '  where "Situacao" = '+ "'nao iniciada'"+ ' group by "numeroOp" ',conn)
    df_OP1 = pd.merge(df_OP1,df_OP_Iniciada,on='numeroOp',how='left')
    df_OP1['qtdpeçs_reposto'] = df_OP1['qtdpeçs_reposto'].replace('', numpy.nan).fillna('0')
    df_OP1['qtdpeçs_total'] = df_OP1['qtdpeçs_total'].astype(int)
    df_OP1['qtdpeçs_reposto'] = df_OP1['qtdpeçs_reposto'].astype(int)
    df_OP1['% Reposto'] = numpy.divide(df_OP1['qtdpeçs_reposto'],df_OP1['qtdpeçs_total'])
    # Clasificando o Dataframe para analise
    df_OP1 = df_OP1.sort_values(by='qtdpeçs_total', ascending=False, ignore_index=True)  # escolher como deseja classificar
    df_OP1["Situacao"] = df_OP1.apply(lambda row: 'Iniciada'  if row['qtdpeçs_reposto'] >0 else 'Nao Iniciada', axis=1)
    # Limitar o número de linhas usando head()
    df_OP1 = df_OP1.head(50) 

    return df_OP1

def AtribuiRepositorOP(codigo, numeroOP):
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    cursor.execute('update "Reposicao"."FilaReposicaoporTag" '
                   'set "Usuario"  = %s where "numeroOp" = %s'
                   , (codigo, numeroOP))
    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    return  numero_linhas_afetadas

def ConsultaSeExisteAtribuicao(numeroOP):
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    cursor.execute('select "numeroOp", "Usuario"  from "Reposicao"."FilaReposicaoporTag" frt  '
                   'WHERE "numeroOp" = %s AND "Usuario" IS NULL', (numeroOP,))
    # Obter o número de linhas afetadas
    NumeroLInhas = cursor.rowcount

    cursor.close()
    conn.close()
    return NumeroLInhas
  
