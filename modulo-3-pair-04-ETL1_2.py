"""Pair ETL I

Ejercicios ETL Parte I

En este caso trabajas en una empresa de venta al por menor de productos italianos y debes realizar la limpieza, transformación e integración de datos de ventas, productos y clientes para su análisis.

Los pasos que deberás seguir en este ejercicio son:

    Lectura de la Información:

        Leer los archivos CSV (ventas.csv, productos.csv, clientes.csv).

        Explorar los conjuntos de datos para comprender su estructura, columnas, tipos de datos, etc.

    Transformación de Datos:

        Limpiar los datos: manejar valores nulos, eliminar duplicados si los hay, corregir errores tipográficos, etc.

        Realizar la integración de datos: unir los conjuntos de datos apropiados para obtener una tabla única que contenga información de ventas junto con detalles de productos y clientes.

        Aplicar transformaciones relevantes según sea necesario: por ejemplo, convertir tipos de datos, renombrar columnas, crear nuevas características derivadas, etc."""

#%%
import pandas as pd
pd.set_option('display.max_columns', None) # para poder visualizar todas las columnas de los DataFrames
import os
import sys
import mysql.connector
from mysql.connector import errorcode
from ast import literal_eval

# %%

ventas = pd.read_csv('data/ventas.csv', on_bad_lines='skip')  # Ajustar delimitador si es necesario
productos = pd.read_csv('data/productos.csv', on_bad_lines='skip',quotechar='"')
clientes = pd.read_csv('data/clientes.csv', on_bad_lines='skip')

# Verificar las primeras filas de cada dataframe
print("Ventas:")
print(ventas.head())

print("\nProductos:")
print(productos.head())

print("\nClientes:")
print(clientes.head())# %%

# %%

ventas.describe()
ventas.info()
ventas.isnull().sum()

ventas["Fecha_Venta"]= pd.to_datetime(ventas["Fecha_Venta"])

# %%


productos.shape


# %%
columns = ['ID', 'Nombre_Producto', 'Categoría', 'Precio', 'Origen', 'Descripción']

# Crear el DataFrame
productos = pd.DataFrame(productos, columns=columns, index= "ID")
# %%
productos.head()

# %%
clientes.info()
# %%
clientes.sample(50)
#%%
def change_null_for_unknown(df, column_list):
    # Iterar a través de la lista de columnas para reemplazar nulos con "Unknown"
    for column in column_list:
        if column in df.columns:
            # Reemplazar nulos con el valor "Unknown" para cada columna en la lista
            df[column] = df[column].fillna("Unknown")
        else:
            print(f"Warning: The column '{column}' does not exist in the DataFrame.")
    return df

#%%
columnas_modificar = ["email","gender","City","Country","Address"]

clientes = change_null_for_unknown(clientes, columnas_modificar)
clientes[columnas_modificar].isnull().sum()

# %%
ventas_clientes = pd.merge(ventas, clientes, how='left', left_on='ID_Cliente', right_on='id')

# Unir el resultado anterior con productos en ID_Producto
ventas_productos_clientes = pd.merge(ventas_clientes, productos, how='left', left_on='ID_Producto', right_on='ID')
# %%
ventas_productos_clientes.head()
# %%

#%%
def insertar_datos(query, contraseña, nombre_bbdd, df):
    """
    Inserta datos en una base de datos utilizando una consulta y una lista de tuplas como valores.

    Args:
    - query (str): Consulta SQL con placeholders para la inserción de datos.
    - contraseña (str): Contraseña para la conexión a la base de datos.
    - nombre_bbdd (str): Nombre de la base de datos a la que se conectará.
    - lista_tuplas (list): Lista que contiene las tuplas con los datos a insertar.

    Returns:
    - None: No devuelve ningún valor, pero inserta los datos en la base de datos.

    This function connects to a MySQL database using the given credentials, executes the query with the provided list of tuples, and commits the changes to the database. In case of an error, it prints the error details.
    """
    cnx = mysql.connector.connect(
        user="root", 
        password=contraseña, 
        host="127.0.0.1", database=nombre_bbdd
    )

    mycursor = cnx.cursor()

    try:
        mycursor.executemany(query, df)
        cnx.commit()
        print(mycursor.rowcount, "registro/s insertado/s.")
        cnx.close()

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)
        cnx.close()

#%%
query_insertar_ventas = """INSERT INTO ventas (ID_Cliente, ID_Producto, Fecha_Venta, Cantidad, Total) VALUES (%s,%s,%s,%s,%s)"""
query_insertar_productos = "INSERT INTO productos (ID, Nombre_Producto, Categoria, Precio, Origen, Descripcion) VALUES (%s, %s,%s,%s,%s,%s)"
query_insertar_clientas = "INSERT INTO clientes (id, first_name, last_name, email, gender, City, Country, Address) VALUES (%s, %s,%s, %s,%s, %s,%s, %s)"

# %%
ventas_tuples = [tuple(x) for x in ventas.to_numpy()]
productos_tuples = [tuple(x) for x in productos.to_numpy()]
clientes_tuples = [tuple(x) for x in clientes.to_numpy()]
#%%
insertar_datos(query_insertar_ventas,"AlumnaAdalab","ventas_productos",ventas_tuples)
#%%
insertar_datos(query_insertar_productos,"AlumnaAdalab","ventas_productos",productos_tuples)
#%%
insertar_datos(query_insertar_clientas,"AlumnaAdalab","ventas_productos",clientes_tuples)