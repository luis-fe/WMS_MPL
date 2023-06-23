import pandas as pd
import ConexaoPostgreRailway
import numpy
import time


# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"
def relatorioEndereços ():
    conn = ConexaoPostgreRailway.conexao()
    relatorioEndereço = pd.read_sql('select "Endereco","codreduzido" ,"engenharia" , count(codbarrastag) as saldo, "descricao", cor , tamanho     from "Reposicao".tagsreposicao t   '
                                    'group by "Endereco", "codreduzido" , "engenharia" ,"descricao", cor , tamanho   ',conn)
    conn.close()
    return relatorioEndereço

def relatorioFila ():
    conn = ConexaoPostgreRailway.conexao()
    relatorioFila = pd.read_sql('select "numeroop", count(codbarrastag) as Saldo, engenharia, descricao from "Reposicao".filareposicaoportag t '
                                'group by "numeroop", "engenharia", "descricao" ',conn)
    conn.close()
    return relatorioFila



def relatorioTotalFila():
    conn = ConexaoPostgreRailway.conexao()
    query = pd.read_sql('SELECT numeroop, COUNT(codbarrastag) AS Saldo '
        'FROM "Reposicao".filareposicaoportag t' 
        ' GROUP BY "numeroop" ',conn)

    query2 = pd.read_sql('select *, 1 as contagem from "Reposicao".pedidossku p'
                        " where endereco = 'Não Reposto'",conn)

    query3 = pd.read_sql('select *, 1 as contagem from "Reposicao".pedidossku p'
                        " where endereco <> 'Não Reposto'",conn)

    query['saldo'] = query['saldo'].sum()
    query2['contagem'] = query2['contagem'].sum()
    query3['contagem'] = query3['contagem'].sum()
    Percentual = round(query2['contagem'][0] / query3['contagem'][0], 2)

    conn.close()
    data = {
        '1-Saldo na Fila':   f'{query["saldo"][0]}',
        '2-Qtd de Enderecos Nao Reposto em Pedido': f'{query2["contagem"][0]}',
        '3-Qtd de Enderecos OK Reposto nos Pedido': f'{query3["contagem"][0]}, percentual reposto {Percentual}%'

    }
    return [data]
print(relatorioTotalFila())