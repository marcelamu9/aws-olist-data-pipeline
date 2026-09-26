# Pipeline Analítico de E-commerce en AWS con Olist

## 1. Descripción del proyecto

Este proyecto implementa un pipeline de ingeniería de datos en AWS para integrar, transformar, almacenar y analizar información del **Brazilian E-Commerce Public Dataset by Olist**.

El objetivo es construir una capa analítica que permita evaluar el desempeño logístico de los pedidos y estudiar su relación con la satisfacción del cliente.

La solución implementa un flujo batch utilizando Amazon S3, AWS Glue, Glue Data Catalog y Amazon Athena. Los datos originales en formato CSV son transformados mediante PySpark y almacenados posteriormente en formato Parquet para su explotación analítica.

> **Estado del proyecto:** v1 funcional — Data Engineering + Analytics.   
> El proyecto continúa en desarrollo con futuras extensiones de visualización, Machine Learning, orquestación e IA generativa.


---

## 2. Escenario de negocio

### Contexto

Una empresa de e-commerce dispone de información distribuida en diferentes fuentes operacionales relacionadas con pedidos, productos, clientes, vendedores, entregas y reseñas.

### Problema

La información fragmentada dificulta analizar de forma integrada el desempeño logístico, identificar retrasos y estudiar su relación con la experiencia del cliente.

### Necesidad de negocio

Construir un pipeline que centralice, limpie y transforme la información histórica para generar datasets analíticos orientados a consultas de negocio.

### Pregunta principal

**¿Cómo se relaciona el desempeño de las entregas con la satisfacción del cliente y qué regiones o categorías presentan mayores problemas logísticos?**

### Extensión de Machine Learning

La arquitectura y el dataset analítico permiten posteriormente entrenar un modelo para estimar la probabilidad de retraso de un pedido utilizando únicamente información disponible antes de conocer el resultado de la entrega.


---

## 3. Dataset

**Fuente:** Brazilian E-Commerce Public Dataset by Olist.

### Tablas consideradas en la V1

| Dataset | Uso |
|---|---|
| `olist_orders_dataset.csv` | Estado y fechas del pedido |
| `olist_order_items_dataset.csv` | Productos, vendedores, precio y flete |
| `olist_customers_dataset.csv` | Identificación y ubicación del cliente |
| `olist_products_dataset.csv` | Categoría y características físicas del producto |
| `olist_order_reviews_dataset.csv` | Puntuación y comentarios de satisfacción |
| `olist_sellers_dataset.csv` | Información del vendedor |
| `product_category_name_translation.csv` | Traducción de categorías al inglés |

Los datasets de pagos y geolocalización forman parte del dataset original, pero no fueron necesarios para responder el objetivo principal de la primera versión.


---

### Modelo de datos

Se realizó un diagrama entidad-relación para identificar llaves lógicas, cardinalidades y relaciones entre las diferentes fuentes.

![Diagrama Entidad-Relación](docs/screenshots/DiagramaOlist.png)

---

## 4. Arquitectura de la solución


![Arquitectura AWS](docs/screenshots/aws_olist_architecture_editable.svg)

El pipeline implementado sigue el flujo:

```text
Olist CSV
   ↓
Amazon S3 - raw
   ↓
AWS Glue Crawler
   ↓
Glue Data Catalog - olist_raw_db
   ↓
AWS Glue Job - PySpark
   ↓
Amazon S3 - processed / Parquet
   ↓
AWS Glue Crawler
   ↓
Glue Data Catalog - olist_analytics_db
   ↓
Amazon Athena
   ↓
Análisis de negocio
``` 

---


### Servicios AWS

| Servicio          | Función y justificación                                                                                                                                                 |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Amazon S3         | Almacena las capas `raw` y `processed`. Se utilizó por su escalabilidad, bajo acoplamiento con el procesamiento y compatibilidad con formatos analíticos como Parquet.  |
| AWS Glue          | Ejecuta el proceso ETL utilizando PySpark. Permite integrar las fuentes, transformar variables y generar los datasets analíticos sin administrar infraestructura Spark. |
| Glue Data Catalog | Registra los esquemas tanto de los datos originales (`olist_raw_db`) como de los datos procesados (`olist_analytics_db`).                                               |
| Amazon Athena     | Ejecuta consultas SQL directamente sobre los archivos Parquet almacenados en S3 sin necesidad de aprovisionar una base de datos analítica.                              |
| Amazon CloudWatch | Almacena logs y permite monitorear las ejecuciones del Glue Job.                                                                                                        |
| AWS IAM           | Controla los permisos del pipeline mediante un rol específico para AWS Glue.                                                                                            |


---

## 5. Organización de datos en S3

Estructura prevista:

``` 
s3://aws-olist-data-pipeline/
│
├── raw/
│   ├── orders/
│   ├── order_items/
│   ├── order_reviews/
│   ├── category_translation/
│   ├── customers/
│   ├── products/
│   └── sellers/
│
├── processed/
│   ├── orders_analytics/
│   └── category_analytics/
│
└── athena-results/
``` 

La capa raw conserva los archivos originales, mientras que processed contiene las salidas generadas por AWS Glue en formato Parquet.

---

## 6. Procesamiento y transformaciones

El procesamiento se implementó mediante un AWS Glue Job desarrollado en PySpark.

El job:

* Lee las tablas registradas en olist_raw_db.
* Convierte columnas de fecha almacenadas originalmente como string a timestamp.
* Integra las distintas fuentes mediante joins.
* Agrega order_items antes del join final para conservar el grano de una fila por pedido.
* Genera variables logísticas, económicas y de producto.
* Agrega las reseñas por pedido.
* Genera dos datasets analíticos.
* Escribe los resultados en formato Parquet en Amazon S3.

### Datasets generados

* orders_analytics
Una fila por pedido.
Contiene 99,441 registros.

Está orientado al análisis logístico, satisfacción y futura construcción de modelos de Machine Learning.

* category_analytics

Una fila por combinación pedido-categoría.
Contiene 99,470 registros.
Permite analizar métricas logísticas y de satisfacción desde la perspectiva de las categorías de producto.

### Variables derivadas principales

| Variable                  | Definición                                                 |
| ------------------------- | ---------------------------------------------------------- |
| `estimated_delivery_days` | Días entre la compra y la fecha estimada de entrega        |
| `actual_delivery_days`    | Días entre la compra y la fecha real de entrega            |
| `delay_days`              | Diferencia entre la entrega real y la fecha estimada       |
| `is_delayed`              | Indicador de pedido entregado después de la fecha estimada |
| `item_count`              | Número de ítems incluidos en el pedido                     |
| `product_count`           | Número de productos distintos                              |
| `seller_count`            | Número de vendedores distintos                             |
| `total_price`             | Valor acumulado de los productos                           |
| `total_freight`           | Valor acumulado del flete                                  |
| `order_value`             | `total_price + total_freight`                              |
| `main_product_category`   | Categoría dominante del pedido                             |
| `avg_product_weight_g`    | Peso promedio de los productos                             |
| `avg_product_volume_cm3`  | Volumen promedio de los productos                          |
| `avg_review_score`        | Puntuación promedio asociada al pedido                     |
| `review_count`            | Cantidad de reseñas asociadas                              |
| `has_review_comment`      | Indica si existe comentario textual                        |
| `purchase_year`           | Año de compra                                              |
| `purchase_month`          | Mes de compra                                              |
| `purchase_day_of_week`    | Día de la semana de la compra                              |

---

## 7. Análisis con Athena

Los datasets procesados son consultados con Amazon Athena utilizando olist_analytics_db.

Las consultas se encuentran versionadas en `athena/queries/`:

```
01_delay_rate.sql
02_reviews_vs_delays.sql
03_delays_by_state.sql
04_delays_by_category.sql
05_delivery_time.sql
```

`Athena/run_query.py` permite ejecutar las consultas desde un entorno local mediante boto3.
`python athena/run_query.py athena/queries/01_delay_rate.sql`
El script envía la consulta a Athena, espera su finalización y muestra los resultados en la terminal.

---

## 8. Resultados
Los análisis realizados hasta el momento muestran:

| Indicador                                 | Resultado |
| ----------------------------------------- | --------: |
| Pedidos totales procesados                |    99,441 |
| Pedidos con información válida de entrega |    96,476 |
| Pedidos retrasados                        |     7,827 |
| Tasa de retraso                           |     8.11% |
| Review promedio — pedidos retrasados      |      2.57 |
| Review promedio — pedidos a tiempo        |      4.29 |

Los pedidos retrasados presentan una calificación promedio 1.72 puntos inferior a los pedidos entregados a tiempo, mostrando una asociación importante entre desempeño logístico y satisfacción del cliente.

En el análisis geográfico, estados como AL y MA presentaron algunas de las mayores tasas de retraso observadas, mientras que estados con mayor volumen como RJ también acumularon una cantidad considerable de entregas tardías.



### 9. Seguridad

Se creó el rol: `AWSGlueServiceRole-OlistPipeline` para permitir que AWS Glue acceda únicamente a los recursos necesarios.

Se configuró una política específica sobre **aws-olist-data-pipeline**, evitando otorgar acceso general mediante AmazonS3FullAccess.

Los permisos contemplan:
* Lectura sobre la capa raw;
* Escritura sobre la capa processed;
* Acceso a Glue Data Catalog;
* Generación de logs del proceso.

El bucket tiene versionado habilitado para conservar versiones de los objetos.

Las credenciales AWS no se almacenan en el repositorio.

---
## 10. Monitoreo

Amazon CloudWatch se utiliza para monitorear la ejecución de los procesos de AWS Glue.

* validar ejecuciones en estado Succeeded
* inspeccionar esquemas PySpark
* verificar transformaciones
* confirmar la generación de 99,441 registros en orders_analytics
* confirmar la generación de 99,470 registros en category_analytics
* diagnosticar errores durante el desarrollo.

---

## 11. Ejecucion del proyecto

### Requisitos
* Python 3
* AWS CLI configurado
* boto3
* credenciales AWS con los permisos correspondientes

### Validar la autenticación:
```aws sts get-caller-identity```

### Ingesta
```python ingestion/upload_raw_to_s3.py```

### Creación del catálogo raw
```python infrastructure/create_glue_catalog.py```

### Configuración del classifier CSV
```python infrastructure/create_csv_classifier.py```

### Creación de tablas con esquema explícito
```python infrastructure/create_manual_glue_tables.py```

### Catálogo analítico
```python infrastructure/create_analytics_catalog.py```

### Consultas Athena
```python athena/run_query.py athena/queries/01_delay_rate.sql```

El Glue Job olist_transform.py debe ejecutarse dentro de AWS Glue debido a su dependencia del runtime de Glue y PySpark.
---

## 12. Estructura del repositorio

```text
aws-olist-data-pipeline/
│
├── README.md
├── .gitignore
│
├── data/
│   ├── README.md
│   └── get_data.py
│
├── ingestion/
│   └── upload_raw_to_s3.py
│
├── infrastructure/
│   ├── create_glue_catalog.py
│   ├── create_csv_classifier.py
│   ├── create_manual_glue_tables.py
│   └── create_analytics_catalog.py
│
├── glue/
│   └── olist_transform.py
│
├── athena/
│   ├── run_query.py
│   └── queries/
│       ├── 01_delay_rate.sql
│       ├── 02_reviews_vs_delays.sql
│       ├── 03_delays_by_state.sql
│       ├── 04_delays_by_category.sql
│       └── 05_delivery_time.sql
│
├── architecture/
│   └── ...
│
└── docs/
    └── screenshots/
```

---
## 14.   Dashboard con Amazon QuickSight
Se construyó un dashboard analítico conectado a `olist_analytics_db` mediante Amazon Athena y SPICE.

El tablero permite explorar:

- tasa de retraso general;
- tiempo promedio de entrega;
- satisfacción promedio;
- retrasos por estado;
- retrasos por categoría;
- patrones mensuales;
- patrones por día de la semana.

![QuickSight Dashboard](docs/screenshots/quicksight_dashboard.png)

---

## 13. Machine Learning  próxima versión

## 13. Machine Learning con Amazon SageMaker

Se implementó un modelo de clasificación con **XGBoost** para estimar la probabilidad de que un pedido llegue después de su fecha estimada de entrega:

`P(is_delayed = 1)`

El objetivo es identificar anticipadamente pedidos con mayor riesgo de retraso, utilizando únicamente información disponible antes de conocer el resultado de la entrega y evitando *data leakage*.

### 13.1. Preparación de datos

Se utilizó la tabla analítica `orders_analytics`, generada por el proceso ETL de AWS Glue y almacenada en Amazon S3 en formato Parquet.

Se seleccionaron **96.476 pedidos con resultado de entrega conocido** y se dividieron en tres conjuntos mediante muestreo estratificado:

| Conjunto      | Registros | Proporción |
| ------------- | --------: | ---------: |
| Entrenamiento |    67.533 |        70% |
| Validación    |    14.471 |        15% |
| Test          |    14.472 |        15% |

La clase de pedidos retrasados representa aproximadamente el **8,11%** de los datos.

El preprocesamiento incluye agrupación de categorías de producto poco frecuentes, imputación de valores faltantes y One-Hot Encoding de variables categóricas. El conjunto de entrenamiento se utiliza para ajustar las transformaciones, que posteriormente se aplican a validación y test.

Las variables incluyen ubicación del cliente, categoría del producto, características del pedido, costo de envío, dimensiones y peso del producto, plazo estimado de entrega y características temporales de la compra.

Se excluyeron variables conocidas después de la entrega, como el tiempo real de entrega, los días de retraso y las calificaciones de los clientes.

### 13.2. Experimentación y evaluación local

Se desarrolló un modelo XGBoost en SageMaker JupyterLab y se evaluó mediante ROC-AUC, PR-AUC, precision, recall y F1-score, considerando el desbalance de clases.

El umbral de clasificación se seleccionó utilizando el conjunto de validación y se fijó en `0.65` antes de la evaluación final sobre test.

El análisis de importancia de variables identificó el mes de compra, el estado del cliente y la categoría principal del producto como las variables con mayor importancia agregada en el modelo. Estas importancias no representan relaciones causales.

### 13.3. Entrenamiento administrado

Los conjuntos preprocesados se exportaron a CSV numérico y se almacenaron en Amazon S3. Se utilizó un **Amazon SageMaker Training Job** con el contenedor administrado de XGBoost y una instancia `ml.m5.large`.

El entrenamiento finalizó correctamente y produjo las siguientes métricas:

| Métrica            | Resultado |
| ------------------ | --------: |
| Train ROC-AUC      |    0.8453 |
| Validation ROC-AUC |    0.7722 |

El modelo entrenado se almacenó automáticamente en Amazon S3 como `model.tar.gz`, sin necesidad de mantener un endpoint activo.

### 13.4. Inferencia con SageMaker Batch Transform

Se creó un modelo de SageMaker a partir del artefacto generado por el Training Job y se ejecutó un **Batch Transform Job** sobre el conjunto de test, utilizando una instancia `ml.m5.large`.

Las predicciones se almacenaron en Amazon S3 y se evaluaron utilizando las etiquetas reales de test y el umbral previamente establecido de `0.65`.

Los resultados fueron:

| Métrica   | XGBoost local | SageMaker XGBoost |
| --------- | ------------: | ----------------: |
| ROC-AUC   |        0.7812 |            0.7798 |
| PR-AUC    |        0.2597 |            0.2588 |
| Precision |        0.2628 |            0.2597 |
| Recall    |        0.4557 |            0.4446 |
| F1-score  |        0.3333 |            0.3279 |

El desempeño del modelo administrado fue similar al obtenido durante la experimentación local. Con el umbral seleccionado, el modelo de SageMaker identificó aproximadamente el **44,5% de los pedidos retrasados**, con una precisión del **26,0%**.

### 13.5. Arquitectura de Machine Learning

```text
Amazon S3 — Processed Parquet
             |
             v
    SageMaker JupyterLab
    EDA y preprocesamiento
             |
             v
    Amazon S3 — ML datasets
       train / validation
             |
             v
  SageMaker Training Job
       Managed XGBoost
             |
             v
 Amazon S3 — model.tar.gz
             |
             v
 SageMaker Batch Transform
             |
             v
 Amazon S3 — Predicciones
             |
             v
 Evaluación sobre test
```

El notebook de experimentación y ejecución se encuentra en [`sagemaker/delay_prediction.ipynb`](sagemaker/delay_prediction.ipynb).

La implementación utiliza entrenamiento e inferencia administrados bajo demanda, sin desplegar un endpoint persistente.


---

## 14. Orquestación con AWS Step Functions

Se implementó una máquina de estados de tipo **Standard** para coordinar la ejecución del pipeline analítico de Olist mediante AWS Step Functions.

El flujo automatiza las siguientes operaciones:

1. Ejecutar el Glue Job `olist-transform-orders` y esperar su finalización.
2. Iniciar el crawler `olist_analytics_crawler` para actualizar el catálogo analítico.
3. Consultar periódicamente el estado del crawler, con intervalos de 30 segundos y un máximo de 20 comprobaciones.
4. Finalizar correctamente cuando el ETL y el crawler hayan terminado con éxito, o registrar un fallo cuando alguno de los procesos falle o se agote el límite de espera.

La máquina utiliza el rol IAM `OlistStepFunctionsExecutionRole`, con permisos específicos para ejecutar y consultar el Glue Job y el crawler.

La definición se encuentra en [`orchestration/state_machine.asl.json`](orchestration/state_machine.asl.json), junto con la política IAM y los archivos utilizados para probar individualmente los estados.

### Validación

Se realizaron pruebas aisladas para comprobar el incremento del contador y las decisiones del crawler, seguidas de una ejecución completa de integración que finalizó en estado **Succeeded**.

![Ejecución exitosa del pipeline en Step Functions](docs/screenshots/stepfunctions_graph.png)

La orquestación se inicia actualmente de forma manual. Como siguientes mejoras se contempla incorporar un disparador automático y controles de calidad de datos antes de dar por completado el pipeline.


---
## 14. Próximas mejoras

- Automatizar la ejecución mediante AWS Step Functions o Glue Workflows.
- Definir la infraestructura mediante Terraform.
- Analizar las reseñas textuales utilizando NLP o Amazon Bedrock.
- Incorporar validaciones adicionales de calidad de datos.
- Evaluar estrategias de particionamiento de los datasets Parquet.

---

## 15. Tecnologías Implementadas

- Amazon S3
- AWS Glue
- AWS Glue Data Catalog
- Amazon Athena
- Amazon SageMaker
- Amazon CloudWatch
- AWS IAM
- Python
- JupyterLab
- PySpark
- SQL
- Parquet
- boto3

Roadmap

* Amazon QuickSight
* Amazon SageMaker
* AWS Step Functions
* Terraform
* Amazon Bedrock