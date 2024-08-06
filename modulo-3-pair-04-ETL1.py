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


productos.head()




# %%
columns = ['ID', 'Nombre_Producto', 'Categoría', 'Precio', 'Origen', 'Descripción']

# Crear el DataFrame
productos = pd.DataFrame(productos, columns=columns)
# %%
productos.head()
# %%
problemas = productos[productos.isnull().any(axis=1)]

# Mostrar filas problemáticas
if not problemas.empty:
    print("\nFilas problemáticas:")
    print(problemas)

# %%

productos_sin_nulos = productos.dropna(subset=['Descripción'])
# %%
productos_sin_nulos.isnull().sum()
# %%
productos[['ID', 'Nombre_Producto']] = productos['ID'].str.split(' ', n=1, expand=True)
# %%
productos.head()
# %%
