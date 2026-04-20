import os
import json
import time
import requests
import subprocess
import snowflake.connector
from tempfile import TemporaryDirectory


def obtener_token() -> str:
    result = subprocess.run(
        [
            "snowsql",
            "--generate-jwt",
            "-a", "HHFXICV-CIVICAPARTNER",
            "-u", "<tu_usuario>",
            "--private-key-path",
            "<path_a_tu_private_key_file>"
        ],
        input="",
        capture_output=True,
        text=True
    )
    token = result.stdout.strip()

    return token

# 1. Conexion a Snowflake
conn = snowflake.connector.connect(
    account="HHFXICV-CIVICAPARTNER",
    user="<tu_usuario>",
    authenticator="externalbrowser"
)

cursor = conn.cursor()
cursor.execute("USE ROLE CURSO_DATA_ENGINEERING")
cursor.execute("USE DATABASE CURSO_DATA_ENGINEERING_2026")
cursor.execute("USE SCHEMA BRONZE")
cursor.execute("SELECT current_user(), current_role(), current_version()")
print(cursor.fetchone())

# 2. Creación de todos los objetos necesarios para la carga de datos
# FILE FORMAT
cursor.execute("CREATE OR REPLACE FILE FORMAT JSON_FORMAT TYPE = JSON STRIP_OUTER_ARRAY = TRUE")
# TABLE
cursor.execute("CREATE OR REPLACE TABLE DOG_BREEDS CLONE GARTIGOT_CURSO_TEST.DIA_3.TEST_BREED")
# PIPE
cursor.execute("""CREATE OR REPLACE PIPE DOG_PIPE 
               AS COPY INTO DOG_BREEDS FROM @LANDING_STG 
               FILE_FORMAT = JSON_FORMAT
               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
               ON_ERROR = 'CONTINUE'""")

# 3. Extraccion y carga a Snowflake
for i in range(1, 11):
    # Extracción
    print(f"Extrayendo raza: {i}/10 de la API.")
    breed = requests.get(url="https://dogapi.dog/api/v2/breeds?page%5Bnumber%5D={i}&page%5Bsize%5D=1")
    if breed.ok:
        print("Datos extraidos correctamente de la API.")
        with TemporaryDirectory() as tmp_folder:
            # Generación de archivo local
            json_file = f"breed{i}.json"
            json_path = os.path.join(tmp_folder, json_file)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(breed.json()['data'], f, indent=4, ensure_ascii=False)

            # Carga a Stage de Snowflake
            upload_sql = (
                "PUT 'file://{path}' @{stage_location}"
            ).format(
                path=json_path.replace("\\", "\\\\").replace("'", "\\'"),
                stage_location="LANDING_STG"
            )
            print(f"Cargando archivo a Snowflake con el comando: '{upload_sql}'")
            cursor.execute(upload_sql)
            os.remove(json_path)

            # Notificación al pipe para que ingeste el nuevo archivo
            # Obtenemos token si es la primera vez que cargamos datos
            if i == 1:
                print('Obteniendo token de acceso...')
                token = obtener_token()
            rest_notification = requests.post(
                url="https://hhfxicv-civicapartner.snowflakecomputing.com/v1/data/pipes/CURSO_DATA_ENGINEERING_2026.BRONZE.DOG_PIPE/insertFiles",
                json={
                    "files": [{"path": f"{json_file}.gz"}]
                },
                headers={"Authorization": f"Bearer {token}"}
            )
            if rest_notification.ok:
                print(f"Archivo {json_file} está siendo ingestado por el pipe.")
            else:
                raise requests.HTTPError(f"Oh oh, el pipe no sabe que el archivo {json_file} se ha subido al stage... no aparecerá en la tabla")
    else:
        requests.HTTPError(f"DEFCON 1. Fallo durante la extracción del dato en la fuente.")
    time.sleep(10)

cursor.close()
conn.close()