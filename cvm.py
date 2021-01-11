#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime

import pandas as pd
from pandas import read_csv
from sqlalchemy import create_engine

def main():
    today = datetime.date.today()
    ano_inicial = 2017
    ano_final = int(today.strftime('%Y'))
    mes_final = int(today.strftime('%m'))

    for ano in range(ano_inicial, ano_final+1):
        for mes in range(1, 13):
            if ano == ano_final and mes > mes_final:
                break

            mes = str(mes).zfill(2)

            processa_arquivo(mes, ano)

    return True


def processa_arquivo(mes, ano):
    url_base = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}{}.csv'
    url = url_base.format(ano, mes)
    print(url)

    try:
        df = pd.read_csv(
            url,
            sep=';',
            encoding='latin1'
        )
    except Exception:
        print('Erro ao baixar arquivo', url)
        return False

    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace('.', '')
    df['CO_PRD'] = df['CO_PRD'].str.replace('/', '')
    df['CO_PRD'] = df['CO_PRD'].str.replace('-', '')
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    df['DT_COMPTC'] = pd.to_datetime(
        df['DT_COMPTC'], infer_datetime_format = True)
    df['DT_REF'] = df['DT_COMPTC']

    engine = create_engine('sqlite:///cvm_inf_diario.db', echo = True)
    sqlite_connection = engine.connect()

    sql_info_diario_table = """CREATE TABLE IF NOT EXISTS Info_Diario_Table (
	    CNPJ_FUNDO TEXT, 
	    DT_COMPTC DATETIME, 
	    VL_TOTAL FLOAT, 
	    VL_QUOTA FLOAT, 
	    VL_PATRIM_LIQ FLOAT, 
	    CAPTC_DIA FLOAT, 
	    RESG_DIA FLOAT, 
	    NR_COTST BIGINT, 
	    CO_PRD TEXT, 
	    DT_REF DATETIME
    );"""

    sqlite_connection.execute(sql_info_diario_table)

    df_db = pd.read_sql_table("Info_Diario_Table", sqlite_connection)

    df_keys = pd.concat([df_db, df], keys=['CO_PRD', 'DT_REF'])

    df_keys.drop_duplicates(
        keep='last',
        inplace=True,
        ignore_index=True,
        subset=['CO_PRD', 'DT_REF']
    )

    del df_db
    del df

    print('Importando dados do Dataframe')
    df_keys.to_sql(
        "Info_Diario_Table",
        sqlite_connection,
        if_exists='replace',
        index=False,
        chunksize=50000
    )
    sqlite_connection.close()

    print(f'{len(df_keys)} Registros importados com sucesso')

    return True

def consulta_dados(cnpj):
    engine = create_engine('sqlite:///cvm_inf_diario.db', echo = True)
    sqlite_connection = engine.connect()

    df_db = pd.read_sql_table("Info_Diario_Table", sqlite_connection)

    result = df_db[df_db['CO_PRD'] == cnpj]

    print(result)

    return True

if __name__ == '__main__':
    main()
