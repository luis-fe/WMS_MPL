import ConexaoPostgreRailway
import pandas as pd
import numpy

def VerificarDuplicacoesDeTagsFila():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select * from "Reposicao".filareposicaoportag f '
                        'where codbarrastag in'
                        ' (select codbarrastag  from ( '
                        ' select count(codbarrastag) as conta, codbarrastag  from "Reposicao".filareposicaoportag f '
                        'group by codbarrastag) as contagem '
                        'where contagem.conta > 1)', conn)
    conn.close()
    if erro1.empty:
        return False
    else:
        return True


