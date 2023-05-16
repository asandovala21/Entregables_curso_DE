# !python3 -m pip install psycopg2
import requests
import pandas as pd
from pandas import json_normalize 
import json
import psycopg2
from psycopg2.extras import execute_values


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
                'object': 'VARCHAR(50)', 'bool': 'BOOLEAN'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    sql_dtypes = [x.replace('VARCHAR(50)', f'VARCHAR({len(max(dataframe[y].fillna(""),key=len))})')
                  if x == 'VARCHAR(50)' else x for x, y in zip(sql_dtypes, cols)]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name,
                   data_type in zip(cols, sql_dtypes)]
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

#API ESCO PUESTOS DE TRABAJO GERENCIALES
api_url = "https://ec.europa.eu/esco/api/search?language=es&type=occupation&text=directivo"
response = requests.get(api_url)
data=response.json()
response.status_code
# print(type(data))
# print(data.get('_embedded').get('results'))
# print(type(data.get('_embedded').get('results')[0].get('title')))
# len(data.get('_embedded').get('results'))
titulos = [data.get('_embedded').get('results')[x].get('title')
           for x in range(len(data.get('_embedded').get('results')))]
codigos = [data.get('_embedded').get('results')[x].get('code')
           for x in range(len(data.get('_embedded').get('results')))]
descripcion = [data.get('_embedded').get('results')[x].get('searchHit')
           for x in range(len(data.get('_embedded').get('results')))]
links = [data.get('_embedded').get('results')[x].get('uri')
                       for x in range(len(data.get('_embedded').get('results')))]

# descripcion = [descripcion[:45]+'...' for descripcion in descripcion]


df = pd.DataFrame({'codigo': codigos, 'titulo': titulos,
                  'descripcion': descripcion, 'link': links})

#API DE WALLSTREETBETS

url = 'https://tradestie.com/api/v1/apps/reddit'
headers = {"Accept-Encoding": "gzip, deflate"}

response = requests.get(url, headers=headers)
data = response.json()

data[0].keys()

num_comentario = [data[x].get('no_of_comments')
           for x in range(len(data))]
sentimiento = [data[x].get('sentiment')
                  for x in range(len(data))]
score_sentimiento = [data[x].get('sentiment_score')
               for x in range(len(data))]
ticker = [data[x].get('ticker')
                     for x in range(len(data))]

df2 = pd.DataFrame({'Nro_comentario': num_comentario, 'Sentimiento': sentimiento,
                  'score_sentimiento': score_sentimiento, 'ticker': ticker})

df2['score_sentimiento']=df2['score_sentimiento'].astype(float)

user = "asandovala_coderhouse"
pwd = "4QWq7d0OSV"
#CARGANDO DATOS EN REDSHIFT
try: 
    cargar_en_redshift(user=user, pwd=pwd, table_name='ocupaciones_gerenciales_esco', dataframe=df)
except Exception as e:
    print("No se puede cargar datos de primera API, así que se cargan datos de segunda")
    cargar_en_redshift(
    user=user, pwd=pwd, table_name='base_wallstreetbets', dataframe=df2)
