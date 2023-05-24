import psycopg2
import ConecaoAWSRS
import ConecaoPostgreRailway

def PesquisaEnderecos (Coluna,Operador,Nome):
    conn = ConecaoPostgreRailway.conexao()

    consulta = f'select "Referencia", "Endereco" from "enderecamento"  WHERE "{Coluna}" {Operador} %s'
    valor = (Nome,)
    cursor = conn.cursor()
    cursor.execute(consulta, valor)
    resultados = []
    for resultado in cursor.fetchall():
        resultados.append(resultado)

    cursor.close()
    conn.close()
    print('REALIZADO CONSULTA DE ENDEREÇO DAS TELAS DE SILK')
    return resultados





def Funcao_Deletar (Endereco,Produto):

    conn = ConecaoPostgreRailway.conexao()
    cursor =conn.cursor()

    sql= 'DELETE FROM "enderecamento" where "Endereco" = %s and "Referencia" = %s  '
    VALORES = (Endereco, Produto,)
    cursor.execute(sql, VALORES)
    conn.commit()
    cursor.close()

    conn.close()
    print('REALIZADO DELETE DE ENDEREÇO DO SILK NO CADASTRO')
    return True

def Funcao_Inserir (Referencia, Endereco):

    conn = ConecaoPostgreRailway.conexao()

    cursor =conn.cursor()

    sql= 'INSERT INTO "enderecamento" ("Referencia", "Endereco") VALUES (%s, %s)'
    VALORES = (Referencia, Endereco,)
    cursor.execute(sql, VALORES)
    conn.commit()
    cursor.close()

    conn.close()
    print('REALIZADO NOVA INSERÇÃO DE ENDEREÇO DO SILK NO CADASTRO')
    return True



#Funcao_InserirTamanhos(2, "Calça Legging", "Calça", "Ativo", "15/05/2023", "joao.ferreira")
