# Orquestación y Monitorización con Streams y Tasks

El objetivo de este ejercicio es automatizar el flujo de transformación de datos usando Streams y Tasks en Snowflake.

Construiremos un pipeline incremental que detecta nuevos pedidos, actualiza tablas intermedias y refresca los agregados automáticamente.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASOS PREVIOS: 

Antes de comenzar, debemos preparar nuestro entorno y crear la base de datos clonada. En primer lugar y para asegurarnos que todos partimos de la misma base, vamos a clonar en nuestra base de datos:

🧠 Nota: Reemplaza MY_DB por el nombre que elijas para tu base de datos personal (Por ejemplo: DEV_CURSO_DB_ALUMNO_XX)

```
USE ROLE CURSO_DATA_ENGINEERING;
USE WAREHOUSE WH_CURSO_DATA_ENGINEERING;

-- 👇 Sustituye MY_DB por el nombre de tu base de datos
USE DATABASE MY_DB;

-- 👇 Crea una base de datos clonada con el nombre que elijas
CREATE OR REPLACE DATABASE MY_DB
CLONE CURSO_DATA_ENGINEERING_TO_BE_CLONED;
```

Ahora crearemos en nuestra Base de Datos y esquema GOLD la siguiente tabla que hace un agregado del número de pedidos y su status por fecha. Se alimenta directamente de la tabla SILVER.ORDERS, que ya contiene la información limpia. 

Ejecuta el siguiente comando (recuerda reemplazar MY_DB por el nombre de tu base de datos):

```
CREATE OR REPLACE TABLE MY_DB.GOLD.ORDERS_STATUS_DATE AS (
    SELECT 
        TO_DATE(CREATED_AT) AS FECHA_CREACION_PEDIDO,
        STATUS,
        COUNT(DISTINCT ORDER_ID) AS NUM_PEDIDOS
    FROM 
        MY_DB.SILVER.ORDERS 
    GROUP BY    
        TO_DATE(CREATED_AT),
        STATUS
    ORDER BY 1 DESC
);
```

#### Relación entre las capas:

```plaintext
BRONZE (crudo) → SILVER (limpio) → GOLD (agregado)
│                 │                 │
│                 │                 └──► ORDERS_STATUS_DATE (número de pedidos por estado y fecha)
│                 └──► ORDERS (datos transformados)
└──► ORDERS_HIST (fuente de pedidos históricos)
```

Lo que vamos a hacer en esta práctica es automatizar este flujo con Streams y Tasks, para que todo el proceso se ejecute sin intervención manual.


¿Y si mejor encapsulamos la creación de esta tabla en un procedimiento almacenado como aprendimos ayer?, así podremos recrearla cada vez que la información se actualice:

```
CREATE OR REPLACE PROCEDURE MY_DB.GOLD.update_gold_orders_status()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN

    CREATE OR REPLACE TABLE MY_DB.GOLD.ORDERS_STATUS_DATE AS (
        SELECT 
            TO_DATE(CREATED_AT) AS FECHA_CREACION_PEDIDO,
            STATUS,
            COUNT(DISTINCT ORDER_ID) AS NUM_PEDIDOS
        FROM 
            MY_DB.SILVER.ORDERS
        GROUP BY    
            TO_DATE(CREATED_AT),
            STATUS
        ORDER BY 1 DESC
    );

    return 'Tabla ORDERS_STATUS_DATE actualizada con éxito';
END;
```

Hasta aquí los pasos previos, ahora vamos con lo nuevo!

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 1: STREAM

En primer lugar vamos a crear el Stream en nuestro esquema BRONZE (tipo **`APPEND ONLY`**) sobre la tabla **`ORDERS_HIST`**, pero ojo!, lo vamos a crear sobre la tabla **`ORDERS_HIST`** que está en la Base de Datos común! (CURSO_DATA_ENGINEERING_TO_BE_CLONED.BRONZE.ORDERS_HIST), no la que tienes en tu propia Base de Datos.

Lo haremos así para simular la entrada de nuevos pedidos, es decir, una vez esté todo configurado insertaremos nosotros a modo de prueba registros y si todo está bien configurado, el pipeline que estáis a punto de construir funcionará a la perfección!.

Recordad ejecutar siempre desde vuestra Base de Datos y crearemos el Stream con el nombre **`ORDERS_STREAM`**.

Será un Stream sobre la tabla:
```
CURSO_DATA_ENGINEERING_TO_BE_CLONED.BRONZE.ORDERS_HIST;
```

Como siempre, la documentación es nuestra amiga:

https://docs.snowflake.com/en/sql-reference/sql/create-stream

👉 Asegúrate de incluir el APPEND_ONLY = TRUE explícito

Todo bien? Si lanzáis SHOW STREAMS lo véis?

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 2: TAREA RAÍZ (ROOT TASK)

En este paso vamos a **construir el árbol de tareas** que formará la base de nuestro **pipeline automatizado**.  El objetivo es que Snowflake ejecute tareas de forma **programada y dependiente**, reaccionando automáticamente cuando haya nuevos datos.

Queremos que generéis la tarea raíz (ROOT TASK) será el punto de partida de nuestro flujo de ejecución.  

Esta tarea se encargará de **detectar nuevos datos en el Stream** y, cuando los haya, **actualizar la tabla `SILVER.ORDERS`** con los registros nuevos o modificados.

### Lógica de ejecución

Queremos que esta tarea funcione de forma **incremental y automática**, es decir:

1. **Se ejecutará cada 30 segundos** gracias a un *scheduler*.  
2. **Solo se activará si el Stream tiene datos nuevos**, usando la función:  
   ```sql
   SYSTEM$STREAM_HAS_DATA('ORDERS_STREAM')
   ```

Así pues, vamos a la documentación a resfrescar cómo podíamos conseguir esto que os pedimos:

https://docs.snowflake.com/en/sql-reference/sql/create-task

¿Y que es lo que va a ejecutar esta task?, buena pregunta... El objetivo principal es **actualizar la tabla `SILVER.ORDERS`** con los nuevos pedidos o con cambios en los existentes, tomando los datos del **Stream `ORDERS_STREAM`**.

> ⚠️ Es muy importante que uses **tu propia base de datos y esquema**, no la tabla de la base común.

### Operación de Merge

Para lograr esto, utilizaremos un **`MERGE`**, que permite:

- Actualizar registros existentes si coinciden (`WHEN MATCHED THEN UPDATE`)
- Insertar nuevos registros si no existen (`WHEN NOT MATCHED THEN INSERT`)

Documentación del Merge --> https://docs.snowflake.com/en/sql-reference/sql/merge

Por tanto, debéis completar este bloque de código:

```
--MERGE
    MERGE INTO MY_DB.SILVER.ORDERS t
    USING 
    (
        SELECT *
        FROM
            MY_DB.BRONZE.ORDERS_STREAM 
    ) s ON t.ORDER_ID = s.ORDER_ID
            WHEN MATCHED THEN UPDATE ...
```

Como sabéis para que no haya errores de tipos de datos, debemos castear algunas columnas del Stream antes de insertarlas o actualizar en la capa SILVER. Para que no perdáis tiempo con los casteos que hicísteis ayer en la tabla de **`ORDERS`**...os los dejamos por aquí:

Nota: Tened en cuenta que los nombres de las columnas son iguales en la tabla **`SILVER.ORDERS`** vs **`MY_DB.BRONZE.ORDERS_STREAM`** excepto para las columnas **`PROMO_NAME`** (ORDERS) vs **`PROMO_ID`** (ORDERS_STREAM)

```
ORDER_ID::varchar(100),
SHIPPING_SERVICE::varchar(20),
replace(SHIPPING_COST,',','.')::decimal,
ADDRESS_ID::varchar(50),
CREATED_AT::timestamp_ntz,
IFNULL(promo_id,'N/A'),
ESTIMATED_DELIVERY_AT::timestamp_ntz,
(replace(ORDER_COST,',','.'))::decimal,
USER_ID::varchar(50),
(replace(s.ORDER_TOTAL,',','.'))::decimal,
DELIVERED_AT::timestamp_ntz,
TRACKING_ID::varchar(50),
STATUS::varchar(20),
TIMESTAMPDIFF(HOUR,created_at,delivered_at)
```

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 3: TAREA HIJA

Para probar el concepto que vimos en la teoría de tarea hija en el árbol de tareas, crearéis una nueva tarea en Bronze (**`TASK_HIJA`**) que se lanzará justo cuando la tarea raíz (**`ROOT_TASK`**) finalice.

En nuestro caso:

- **Tarea raíz (ROOT_TASK)** → actualiza los datos de `SILVER.ORDERS` desde el Stream.
- **Tarea hija (TASK_HIJA)** → actualiza los agregados en `GOLD.ORDERS_STATUS_DATE` llamando al procedimiento almacenado.

Para definir que una tarea dependa de otra, usamos la cláusula `AFTER` seguida del nombre de la tarea predecesora.   Esto asegura que la tarea hija **solo se ejecute después de que la tarea raíz haya terminado**, sin importar si la ejecución fue exitosa o no (aunque normalmente queremos que sea después de `SUCCEEDED`).

Documentación para este paso --> https://docs.snowflake.com/en/sql-reference/sql/create-task

En este caso la tarea hija ejecutará el procedimiento almacenado que hemos creado en las tareas previas:
update_gold_orders_status()

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 4: ACTIVA TUS TAREAS

Una vez creadas las tareas, no olvides activarlas, para ello:

```
ALTER TASK IF EXISTS MY_SCHEMA.TASK_HIJA RESUME;
ALTER TASK IF EXISTS MY_SCHEMA.ROOT_TASK RESUME;
```

¿Por qué se hace `RESUME` primero en la tarea hija?

En Snowflake, **todas las tareas dependientes deben existir y estar activas para que la tarea raíz pueda ejecutar la cadena completa**.  Sin embargo, si activamos primero la raíz y la hija aún no existe o está suspendida, la raíz no podrá disparar correctamente la tarea hija.  

Para comprobar si la tarea raíz (ROOT_TASK) está comprobando cada 30 segundos, que es el tiempo que le configuramos, si el Stream tiene datos o no, podemos lanzar una consulta en el task history y comprobarlo:

```
--CHECK TASK HISTORY
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'ROOT_TASK';
```

El campo STATE aparecerá como SCHEDULED y tras cada revisión, como todavía no hemos insertado datos en el ORDERS_HIST, pasará a estado SKIPPED hasta que insertemos datos y entonces debería ser SUCCEEDED (ojalá) o FAILED (si algo hay mal).

| Estado      | Significado                                                        |
|------------|-------------------------------------------------------------------|
| `SCHEDULED` | La tarea está programada y esperando su turno para ejecutarse.    |
| `SKIPPED`   | La tarea revisó el Stream pero no había datos nuevos.             |
| `SUCCEEDED` | La tarea se ejecutó correctamente y realizó los cambios esperados.|
| `FAILED`    | Hubo un error durante la ejecución (revisar logs y SQL).          |

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 5: COMPROBACIÓN

Ahora nosotros nos vamos a encargar de insertar valores en la tabla de la base de datos común a la que apunta el Stream que has creado, como si estuvieran entrando y actualizándose pedidos y si todo va bien, los SPs de las tareas no fallan y la magia del SQL hace su función, deberás ver como se insertan en la tabla **`ORDERS`** de tu esquema SILVER y como la tabla de GOLD va mostrando los cambios.

```
SELECT * FROM GOLD.ORDERS_STATUS_DATE;
```


#### Diagrama del flujo del pipeline

- `[CURSO_DATA_ENGINEERING_TO_BE_CLONED.BRONZE.ORDERS_HIST]`
  - ↓ *(Stream)*
- `[MY_DB.BRONZE.ORDERS_STREAM]`
  - ↓ *(Root Task)*
- `[MY_DB.SILVER.ORDERS]`
  - ↓ *(Child Task)*
- `[MY_DB.GOLD.ORDERS_STATUS_DATE]`

Si has llegado hasta aquí...enhorabuena!! Has aprendido una parte importante y muy útil para la ingesta y transformación de la info gracias a la versatilidad que las Streams+Tasks nos aportan en Snowflake.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

### PASO 6: DESACTIVAR TUS TAREAS Y ELIMINARLAS

Una vez terminada la práctica...No olvidéis desactivar vuestras tareas y eliminarlas!, Gracias!!

```
ALTER TASK MY_DB.BRONZE.ROOT_TASK SUSPEND;
ALTER TASK MY_DB.BRONZE.TASK_HIJA SUSPEND;

DROP TASK MY_DB.BRONZE.ROOT_TASK;
DROP TASK MY_DB.BRONZE.TASK_HIJA;
```

Recuerda: al activar las tareas hazlo de abajo hacia arriba (primero la hija y luego la raíz), y al suspenderlas al revés (primero la raíz y luego la hija), para evitar ejecuciones colgantes en el pipeline.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------------------------------
