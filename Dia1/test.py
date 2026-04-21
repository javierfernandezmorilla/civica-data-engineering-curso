"""Este es un script pensado para experimentar como funciona al conector de Snowflake
para Python. Su estructura basica es:

1. Conexion a Snowflake a través de un navegador web (se abre automáticamente).
2. Uso de un **cursor** para la selección del role, base de datos y schema apropiados.
3. Carga de un archivo local a un Internal Stage a través de comando PUT.

Este script es configurable, es decir, primero debes leerlo y modificarlo antes de ejecutarlo.
Lo que tienes que modificar se indica entre símbolos "<>" y es autoexplicativo.

Enjoy.
"""
import os
import snowflake.connector


# 1. Conexion a Snowflake
conn = snowflake.connector.connect(
    account="HHFXICV-CIVICAPARTNER",
    user="<tu_usuario>",
    authenticator="externalbrowser"
)

# 2. Selección de la base de datos, schema y role apropiados
cursor = conn.cursor()
cursor.execute("USE ROLE CURSO_DATA_ENGINEERING")
cursor.execute("USE DATABASE <tu_bd>")
cursor.execute("USE SCHEMA BRONZE")
cursor.execute("SELECT current_user(), current_role(), current_version()") # Comprobación
print(cursor.fetchone())

# 3. Carga de un archivo local a un internal Stage de Snowflake (PUT)
file_path = "<RUTA COMPLETA AL CSV LOCAL>"
upload_sql = (
    "PUT 'file://{path}' @{stage_location}"
).format(
    path=file_path.replace("\\", "\\\\").replace("'", "\\'"),
    stage_location="<tu_stage>"
)
print(f"Cargando archivo a Snowflake con el comando: '{upload_sql}'")
cursor.execute(upload_sql)
os.remove(file_path)