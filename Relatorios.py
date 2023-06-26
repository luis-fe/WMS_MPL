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
                        " where endereco = 'Não Reposto' and necessidade > 0",conn)

    query3 = pd.read_sql('select *, 1 as contagem from "Reposicao".pedidossku p'
                        " where endereco <> 'Não Reposto' and necessidade > 0",conn)
    Inventario = pd.read_sql('select codreduzido  from "Reposicao".tagsreposicao_inventario ti' ,conn)
    Reposto = pd.read_sql('select codreduzido  from "Reposicao".tagsreposicao ti' ,conn)

    query['saldo'] = query['saldo'].sum()
    query2['contagem'] = query2['contagem'].sum()
    query3['contagem'] = query3['contagem'].sum()
    Inventario['codreduzido'] = Inventario['codreduzido'].count()
    Reposto['codreduzido'] = Reposto['codreduzido'].count()
    total =  query3['contagem'][0] +  query2['contagem'][0]
    Percentual = query3['contagem'][0] / total
    Percentual = round(Percentual, 2) * 100
    totalPecas = query["saldo"][0] + Reposto["codreduzido"][0]+Inventario["codreduzido"][0]
    conn.close()
    data = {
        '1.0':' Informacoes Gerais do Estoque',
        '1.1-Total de Peças Nat. 5':   f'{totalPecas} pçs',
        '1.2-Saldo na Fila':   f'{query["saldo"][0]} pçs',
        '1.3-Peçs em Inventario':   f'{Reposto["codreduzido"][0]} pçs',
        '1.4-Peçs em Inventario':   f'{Inventario["codreduzido"][0]} pçs',
        '2.0':' Informacoes dos pedidos',
        '2.1- Total de Skus nos Pedidos em aberto ': f'{total} pçs',
        '2.2-Qtd de Enderecos Nao Reposto em Pedido': f'{query2["contagem"][0]}',
        '2.3-Qtd de Enderecos OK Reposto nos Pedido': f'{query3["contagem"][0]}',
        '2.4- Percentual Reposto':f'{Percentual}%'
    }
    return [data]


def RelatorioNecessidadeReposicao():
    conn = ConexaoPostgreRailway.conexao()
    relatorioEndereço = pd.read_sql('select produto as codreduzido , sum(necessidade) as necessidade_Pedidos, count(codpedido) as Qtd_Pedidos  from "Reposicao".pedidossku p '
                                    "where necessidade > 0 and endereco = 'Não Reposto'"
                                    " group by produto ",conn)
    relatorioEndereçoEpc = pd.read_sql('select codreduzido , max(epc) as epc_Referencial from "Reposicao".filareposicaoportag f '
                                       'group by codreduzido',conn)

    relatorioEndereço = pd.merge(relatorioEndereço,relatorioEndereçoEpc,on='codreduzido',how='left')
    # Clasificando o Dataframe para analise
    relatorioEndereço = relatorioEndereço.sort_values(by='necessidade_Pedidos', ascending=False, ignore_index=True)  # escolher como deseja classificar
                                    
    conn.close()
    data = {
                
                '1- Detalhamento das Necessidades ':relatorioEndereço.to_dict(orient='records')
            }

    return [data]
    
