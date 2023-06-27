# !python3 -m pip install psycopg2
import requests
import pandas as pd
from pandas import json_normalize 
import json
import psycopg2
from psycopg2.extras import execute_values
import missingno as msno
import numpy as np
import os

def cargar_en_redshift(user, pwd, table_name, dataframe):
    data_base = "data-engineer-database"
    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conexión a Redshift exitosa!")

    except Exception as e:
        print("No se pudo conectar a Redshift.")
        print(e)
    
    
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT', 'int32': 'INT', 'float64': 'FLOAT',
                'object': 'VARCHAR(50)', 'bool': 'BOOLEAN', 'datetime64[ns, UTC]': 'TIMESTAMP'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    sql_dtypes = [x.replace('VARCHAR(50)', f'VARCHAR({len(max(dataframe[y].fillna(""),key=len))})')
                  if x == 'VARCHAR(50)' else x for x, y in zip(sql_dtypes, cols)]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name,
                   data_type in zip(cols, sql_dtypes)]
    name=cols[0]
    primary_key=[f"primary key({name})"]
    column_defs.append(primary_key[0])
    # Combine column definitions into the CREATE TABLE statement
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    # Crear la tabla
    cur = conn.cursor()
    cur.execute(table_schema)
    # Generar los valores a insertar
    values = [tuple(x) for x in dataframe.to_numpy()]
    
    # for i in range(4):
    #     print(f'Valores de {i}')
    #     for j in range(len(values)):
    #         print(len(values[j][i]))
        
    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    # Execute the transaction to insert the data
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')


#API de datos de países
url = "https://restcountries.com/v3.1/all"
response = requests.get(url)
data=response.json()
response.status_code
#explorando la api
# print(data[0].get('name').get('common'))
# print(data[0].get('name').get('official'))
# print(data[0].get('independent'))
# print(type(data[0].get('currencies')))
# #primera moneda
# next(iter(data[0].get('currencies')))
# list(data[0].get('currencies').keys())[0]
# list(data[0].get('currencies').values())[0].get('name')
# print(data[0].get('capital')[0])
# print(data[0].get('region'))
# print(data[0].get('subregion'))
# list(data[0].get('languages').values())[0]
# print(data[0].get('maps').get('googleMaps'))
# print(data[0].get('population'))
# list(data[0].get('gini').values())[0]
# list(data[0].get('gini').keys())[0]
# print(data[0].get('continents')[0])


nombre_comun= [data[x].get('name').get('common')
           for x in range(len(data))]
nombre_oficial= [data[x].get('name').get('official')
           for x in range(len(data))]
pais_independiente= [data[x].get('independent')
           for x in range(len(data))]
moneda_nombre = [list(data[x].get('currencies').values())[0].get('name')
                 if data[x] is not None and data[x].get('currencies') is not None
                 else None
                 for x in range(len(data))]
moneda_simbolo= [list(data[x].get('currencies').keys())[0]
                 if data[x] is not None and data[x].get('currencies') is not None
                 else None
           for x in range(len(data))]
capital= [data[x].get('capital')[0]
          if data[x] is not None and data[x].get('capital') is not None
                 else None
           for x in range(len(data))]
region= [data[x].get('region')
           for x in range(len(data))]
subregion= [data[x].get('subregion')
           for x in range(len(data))]
continente= [data[x].get('continents')[0]
           for x in range(len(data))]
lenguaje=[list(data[x].get('languages').values())[0]
          if data[x] is not None and data[x].get('languages') is not None
                 else None
           for x in range(len(data))]
url_googe_maps=[data[x].get('maps').get('googleMaps')
           for x in range(len(data))]
poblacion=[data[x].get('population')
           for x in range(len(data))]
gini_valor=[list(data[x].get('gini').values())[0]
            if data[x] is not None and data[x].get('gini') is not None
                 else None
           for x in range(len(data))]
gini_year=[list(data[x].get('gini').keys())[0]
           if data[x] is not None and data[x].get('gini') is not None
                 else None
           for x in range(len(data))]

df = pd.DataFrame({'Nombre_comun': nombre_comun,'Nombre_oficial': nombre_oficial,'Pais_independiente': pais_independiente,'Moneda_nombre': moneda_nombre,'Moneda_simbolo': moneda_simbolo,'Capital': capital,'Region': region,'Subregion': subregion,'Continente': continente,'Lenguaje': lenguaje,'Url_googe_maps': url_googe_maps,'Poblacion': poblacion,'Gini_valor': gini_valor,'Gini_year': gini_year})

dag_path = os.getcwd()
df.to_csv(
    f'{dag_path}/raw_data/data_.csv',
    index=False,
)


dt = pd.read_csv(f'{dag_path}/raw_data/data_.csv')

#Explorando el dataframe
df.head()
#Revisar los tipos de datos
df.dtypes
# #Cambiar el tipo de dato de la columna poblacion a int
# df['poblacion'] = df['poblacion'].astype('int64')
# #Cambiar el tipo de dato de la columna gini valor a float
# df['gini_valor'] = df['gini_valor'].astype('float64')
df['Pais_independiente'] = df['Pais_independiente'].astype('bool').astype('int64')

#Revisar si hay missing
msno.matrix(df)
#Revisar si hay duplicados
df.duplicated().sum()
#Revisar otra vez si hay nulos
df.isnull().sum()
df.info()
df.describe()
df = df.drop_duplicates()


df_2=df.groupby('Subregion').agg(suma_poblacion=('Poblacion',np.sum), cantidad_paises=('Nombre_comun', np.size),n_paises_independientes=('Pais_independiente',np.sum),pais_max_poblacion=('Poblacion',np.max)).reset_index()

df_aux=df[['Nombre_comun','Poblacion']]
df_2=df_2.merge(df_aux,how='left',left_on=['pais_max_poblacion'],right_on=['Poblacion'])
df_2.drop(columns=['Poblacion'],inplace=True)
df_2.rename(columns={'Nombre_comun':'pais_max_poblacion_nombre'},inplace=True)
del(df_aux)
user = "asandovala_coderhouse"
pwd = "4QWq7d0OSV"
#CARGANDO DATOS EN REDSHIFT

cargar_en_redshift(
    user=user, pwd=pwd, table_name='base_paises', dataframe=df)
cargar_en_redshift(
    user=user, pwd=pwd, table_name='base_paises_resumen', dataframe=df_2)