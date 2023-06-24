import ConexaoPostgreRailway
import pandas as pd

def DataFrameAtualizar():
    conn = ConexaoPostgreRailway.conexao()
    dataframe = pd.read_sql('select p.codpedido ,p.produto, n.endereco  from "Reposicao".pedidossku p '
                            'inner join "Reposicao".necessidadeendereco n on n.codpedido = p.codpedido and n.produto = p.produto '
                            " where p.endereco = 'Não Reposto'",conn)
    conn.close()
    return dataframe
def testeAtualizacao(iteracoes):
    dataframe = DataFrameAtualizar()
    tamanho = dataframe['codpedido'].size
    conn = ConexaoPostgreRailway.conexao()
    if tamanho <= iteracoes:
        for i in range(iteracoes):
            print(f'incremento: {i}')
            query = '''
                UPDATE "Reposicao".pedidossku p 
                SET "endereco" = %s
                WHERE p.endereco = 'Não Reposto' AND p.codpedido = %s AND p.produto = %s
            '''

            # Execute a consulta usando a conexão e o cursor apropriados
            cursor = conn.cursor()
            cursor.execute(query, (dataframe['endereco'][i],dataframe['codpedido'][i], dataframe['produto'][i]))
            conn.commit()

    else:
        for i in range(tamanho):
            print(f'incremento: {i}')
            query = '''
                UPDATE "Reposicao".pedidossku p 
                SET "endereco" = %s
                WHERE p.endereco = 'Não Reposto' AND p.codpedido = %s AND p.produto = %s
            '''

            # Execute a consulta usando a conexão e o cursor apropriados
            cursor = conn.cursor()
            cursor.execute(query, (dataframe['endereco'][i], dataframe['codpedido'][i], dataframe['produto'][i]))
            conn.commit()