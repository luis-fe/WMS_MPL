import ConexaoPostgreRailway
import pandas as pd

def AtribuirPedido(usuario, pedidos, dataAtribuicao):
    data = {
        '1- Usuario:': usuario,
        '2- Pedidos para Atribuir:': pedidos,
        '3- dataAtribuicao:': dataAtribuicao
    }
    return [data]

