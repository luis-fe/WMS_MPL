import ConexaoPostgreRailway
import pandas as pd

def AtribuirPedido(usuario, pedidos, dataAtribuicao):
    tamanho = len(pedidos)

    pedidos = [p.replace(',', '/') for p in pedidos]

    if tamanho >= 0:
        conn = ConexaoPostgreRailway.conexao()
        for i in range(tamanho):
            pedido_x = str(pedidos[i])
            query = 'update "Reposicao".filaseparacaopedidos '\
                    'set cod_usuario = %s '\
                    'where agrupamentopedido = %s'
            cursor = conn.cursor()
            cursor.execute(query, (usuario,pedido_x, ))
            conn.commit()
            cursor.close()
        conn.close()
    else:
        print('sem pedidos')

    data = {
        '1- Usuario:': usuario,
        '2- Pedidos para Atribuir:': pedidos,
        '3- dataAtribuicao:': dataAtribuicao
    }
    return [data]

