# SRI_Vehiculos_ETL_DAG.py
# Implementación completa del proceso ETL para datos vehiculares del SRI

import warnings
warnings.filterwarnings("ignore")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  
from airflow.operators.empty import EmptyOperator
import pandas as pd
from google.cloud import storage, bigquery
import hashlib
from io import StringIO
import logging
DummyOperator = EmptyOperator


# Configuración por defecto del DAG
default_args = {
    'owner': 'sri_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Definición del DAG
dag = DAG(
    'sri_vehiculos_etl_proceso',
    default_args=default_args,
    description='Proceso ETL completo para datos vehiculares del SRI',
    schedule_interval='@daily',
    catchup=False,
    tags=['sri', 'vehiculos', 'etl', 'bigquery'],
    max_active_runs=1
)

# Variables de configuración
PROJECT_ID = 'sri-vehiculos-etl'  # Reemplazar con tu project ID
DATASET_ID = 'sri_vehiculos_dw'
BUCKET_NAME = 'sri-vehiculos-etl-bucket-angel'  # Reemplazar con tu bucket

# ===============================
# FUNCIONES ETL PARA DIMENSIONES
# ===============================

def etl_dim_tiempo(**context):
    """
    Proceso ETL para la dimensión Tiempo
    Genera un rango completo de fechas desde 2020 hasta 2025
    """
    try:
        logging.info("🕐 Iniciando ETL para Dim_Tiempo...")
        
        # Configurar cliente de BigQuery
        client = bigquery.Client(project=PROJECT_ID)
        
        # Generar rango de fechas
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2025, 12, 31)
        fechas = pd.date_range(start=start_date, end=end_date, freq='D')
        
        logging.info(f"📅 Generando {len(fechas)} registros de fechas...")
        
        # Crear DataFrame de dimensión tiempo
        dim_tiempo = pd.DataFrame({
            'ID_Tiempo': range(1, len(fechas) + 1),
            'FechaCompleta': fechas.date,
            'Anio': fechas.year,
            'Trimestre': fechas.quarter,
            'Mes': fechas.month,
            'Dia': fechas.day,
            'NombreMes': fechas.strftime('%B'),
            'NombreDiaSemana': fechas.strftime('%A')
        })
        
        # Traducir nombres al español
        meses_es = {
            'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo',
            'April': 'Abril', 'May': 'Mayo', 'June': 'Junio',
            'July': 'Julio', 'August': 'Agosto', 'September': 'Septiembre',
            'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
        }
        
        dias_es = {
            'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
            'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado',
            'Sunday': 'Domingo'
        }
        
        dim_tiempo['NombreMes'] = dim_tiempo['NombreMes'].map(meses_es)
        dim_tiempo['NombreDiaSemana'] = dim_tiempo['NombreDiaSemana'].map(dias_es)
        
        # Cargar a BigQuery
        table_id = f'{PROJECT_ID}.{DATASET_ID}.dim_tiempo'
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("ID_Tiempo", "INTEGER"),
                bigquery.SchemaField("FechaCompleta", "DATE"),
                bigquery.SchemaField("Anio", "INTEGER"),
                bigquery.SchemaField("Trimestre", "INTEGER"),
                bigquery.SchemaField("Mes", "INTEGER"),
                bigquery.SchemaField("Dia", "INTEGER"),
                bigquery.SchemaField("NombreMes", "STRING"),
                bigquery.SchemaField("NombreDiaSemana", "STRING"),
            ]
        )
        
        job = client.load_table_from_dataframe(dim_tiempo, table_id, job_config=job_config)
        job.result()  # Esperar a que termine
        
        logging.info(f"✅ Cargados {len(dim_tiempo)} registros en dim_tiempo")
        return f"Dim_Tiempo cargada exitosamente: {len(dim_tiempo)} registros"
        
    except Exception as e:
        logging.error(f"❌ Error en ETL Dim_Tiempo: {str(e)}")
        raise

def etl_dim_vehiculo(**context):
    """
    Proceso ETL para la dimensión Vehículo
    Extrae características únicas de vehículos del archivo CSV
    """
    try:
        logging.info("🚗 Iniciando ETL para Dim_Vehiculo...")
        
        # Configurar clientes
        storage_client = storage.Client()
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        
        # Extraer datos del bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob('raw-data/sri_vehiculos.csv')
        
        # Leer CSV desde Cloud Storage
        content = blob.download_as_text()
        df = pd.read_csv(StringIO(content))
        
        logging.info(f"📊 Datos extraídos: {len(df)} registros originales")
        
        # Seleccionar columnas para la dimensión vehículo
        columnas_vehiculo = [
            'CÓDIGO DE VEHÍCULO', 'MARCA', 'MODELO', 'PAÍS', 
            'AÑO MODELO', 'CLASE', 'SUB CLASE', 'TIPO',
            'CILINDRAJE', 'TIPO COMBUSTIBLE', 'COLOR 1', 'COLOR 2'
        ]
        
        # Verificar que las columnas existen
        columnas_existentes = [col for col in columnas_vehiculo if col in df.columns]
        if len(columnas_existentes) != len(columnas_vehiculo):
            logging.warning(f"Algunas columnas no encontradas. Usando: {columnas_existentes}")
        
        # Crear dimensión con registros únicos
        dim_vehiculo = df[columnas_existentes].drop_duplicates().reset_index(drop=True)
        
        # Generar clave subrogada
        dim_vehiculo['ID_Vehiculo'] = range(1, len(dim_vehiculo) + 1)
        
        # Limpiar y estandarizar datos
        for col in ['MARCA', 'MODELO', 'PAÍS', 'CLASE', 'SUB CLASE', 'TIPO', 'TIPO COMBUSTIBLE']:
            if col in dim_vehiculo.columns:
                dim_vehiculo[col] = dim_vehiculo[col].astype(str).str.upper().str.strip()
        
        # Manejar valores nulos
        if 'COLOR 2' in dim_vehiculo.columns:
            dim_vehiculo['COLOR 2'] = dim_vehiculo['COLOR 2'].fillna('N/A')
        
        # Renombrar columnas para BigQuery (sin espacios ni caracteres especiales)
        rename_dict = {
            'CÓDIGO DE VEHÍCULO': 'CodigoVehiculo',
            'MARCA': 'Marca',
            'MODELO': 'Modelo',
            'PAÍS': 'Pais',
            'AÑO MODELO': 'AnioModelo',
            'CLASE': 'Clase',
            'SUB CLASE': 'SubClase',
            'TIPO': 'Tipo',
            'CILINDRAJE': 'Cilindraje',
            'TIPO COMBUSTIBLE': 'TipoCombustible',
            'COLOR 1': 'Color1',
            'COLOR 2': 'Color2'
        }
        
        # Solo renombrar columnas que existen
        rename_dict_filtered = {k: v for k, v in rename_dict.items() if k in dim_vehiculo.columns}
        dim_vehiculo = dim_vehiculo.rename(columns=rename_dict_filtered)
        
        # Reordenar columnas (solo las que existen)
        columnas_orden = ['ID_Vehiculo'] + [v for k, v in rename_dict_filtered.items()]
        dim_vehiculo = dim_vehiculo[columnas_orden]
        
        logging.info(f"🔧 Transformación completada: {len(dim_vehiculo)} vehículos únicos")
        
        # Cargar a BigQuery
        table_id = f'{PROJECT_ID}.{DATASET_ID}.dim_vehiculo'
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        job = bigquery_client.load_table_from_dataframe(dim_vehiculo, table_id, job_config=job_config)
        job.result()
        
        logging.info(f"✅ Cargados {len(dim_vehiculo)} registros en dim_vehiculo")
        return f"Dim_Vehiculo cargada exitosamente: {len(dim_vehiculo)} registros"
        
    except Exception as e:
        logging.error(f"❌ Error en ETL Dim_Vehiculo: {str(e)}")
        raise

def etl_dim_transaccion(**context):
    """
    Proceso ETL para la dimensión Transacción
    Crea combinaciones únicas de tipos de transacción y servicio
    """
    try:
        logging.info("💼 Iniciando ETL para Dim_Transaccion...")
        
        # Configurar clientes
        storage_client = storage.Client()
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        
        # Extraer datos del bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob('raw-data/sri_vehiculos.csv')
        
        content = blob.download_as_text()
        df = pd.read_csv(StringIO(content))
        
        # Seleccionar columnas para dimensión transacción
        columnas_transaccion = [
            'TIPO TRANSACCIÓN', 'TIPO SERVICIO', 
            'PERSONA NATURAL - JURÍDICA', 'CATEGORÍA'
        ]
        
        # Verificar columnas existentes
        columnas_existentes = [col for col in columnas_transaccion if col in df.columns]
        logging.info(f"Columnas encontradas: {columnas_existentes}")
        
        # Crear dimensión con combinaciones únicas
        dim_transaccion = df[columnas_existentes].drop_duplicates().reset_index(drop=True)
        
        # Generar clave subrogada
        dim_transaccion['ID_Transaccion'] = range(1, len(dim_transaccion) + 1)
        
        # Limpiar datos
        for col in columnas_existentes:
            if col in dim_transaccion.columns:
                dim_transaccion[col] = dim_transaccion[col].astype(str).str.upper().str.strip()
        
        # Renombrar columnas
        rename_dict = {
            'TIPO TRANSACCIÓN': 'TipoTransaccion',
            'TIPO SERVICIO': 'TipoServicio',
            'PERSONA NATURAL - JURÍDICA': 'PersonaTipo',
            'CATEGORÍA': 'Categoria'
        }
        
        rename_dict_filtered = {k: v for k, v in rename_dict.items() if k in dim_transaccion.columns}
        dim_transaccion = dim_transaccion.rename(columns=rename_dict_filtered)
        
        # Reordenar columnas
        columnas_orden = ['ID_Transaccion'] + [v for k, v in rename_dict_filtered.items()]
        dim_transaccion = dim_transaccion[columnas_orden]
        
        logging.info(f"🔧 Transformación completada: {len(dim_transaccion)} tipos de transacción únicos")
        
        # Cargar a BigQuery
        table_id = f'{PROJECT_ID}.{DATASET_ID}.dim_transaccion'
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        job = bigquery_client.load_table_from_dataframe(dim_transaccion, table_id, job_config=job_config)
        job.result()
        
        logging.info(f"✅ Cargados {len(dim_transaccion)} registros en dim_transaccion")
        return f"Dim_Transaccion cargada exitosamente: {len(dim_transaccion)} registros"
        
    except Exception as e:
        logging.error(f"❌ Error en ETL Dim_Transaccion: {str(e)}")
        raise

def etl_dim_ubicacion(**context):
    """
    Proceso ETL para la dimensión Ubicación
    Mapea códigos de cantón a información geográfica completa
    """
    try:
        logging.info("🌎 Iniciando ETL para Dim_Ubicacion...")
        
        # Configurar clientes
        storage_client = storage.Client()
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        
        # Extraer datos del bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob('raw-data/sri_vehiculos.csv')
        
        content = blob.download_as_text()
        df = pd.read_csv(StringIO(content))
        
        # Mapeo de cantones expandido
        mapeo_cantones = {
            '10701': {'canton': 'CUENCA', 'provincia': 'AZUAY', 'region': 'SIERRA'},
            '10911': {'canton': 'GIRON', 'provincia': 'AZUAY', 'region': 'SIERRA'},
            '10901': {'canton': 'GUALACEO', 'provincia': 'AZUAY', 'region': 'SIERRA'},
            '10927': {'canton': 'SANTA ISABEL', 'provincia': 'AZUAY', 'region': 'SIERRA'},
            '20606': {'canton': 'PLAYAS', 'provincia': 'GUAYAS', 'region': 'COSTA'},
            '21101': {'canton': 'GUAYAQUIL', 'provincia': 'GUAYAS', 'region': 'COSTA'},
            '21709': {'canton': 'MILAGRO', 'provincia': 'GUAYAS', 'region': 'COSTA'},
            '31905': {'canton': 'ZAMORA', 'provincia': 'ZAMORA CHINCHIPE', 'region': 'AMAZONIA'},
            '20501': {'canton': 'QUITO', 'provincia': 'PICHINCHA', 'region': 'SIERRA'},
            '20505': {'canton': 'CAYAMBE', 'provincia': 'PICHINCHA', 'region': 'SIERRA'},
            '30101': {'canton': 'LAGO AGRIO', 'provincia': 'SUCUMBIOS', 'region': 'AMAZONIA'},
            '30201': {'canton': 'GONZALO PIZARRO', 'provincia': 'SUCUMBIOS', 'region': 'AMAZONIA'},
            '30301': {'canton': 'PUTUMAYO', 'provincia': 'SUCUMBIOS', 'region': 'AMAZONIA'},
            '30401': {'canton': 'SHUSHUFINDI', 'provincia': 'SUCUMBIOS', 'region': 'AMAZONIA'},
            '30501': {'canton': 'SUCUMBIOS', 'provincia': 'SUCUMBIOS', 'region': 'AMAZONIA'},
            '30601': {'canton': 'CASCALES', 'provincia': 'SUCUMBIOS', 'region': 'AMAZONIA'},
            '30701': {'canton': 'CUYABENO', 'provincia': 'SUCUMBIOS', 'region': 'AMAZONIA'},
        }
        
        # Verificar si la columna CANTON existe
        col_canton = None
        for col in ['CANTON', 'CANTÓN', 'canton', 'cantón']:
            if col in df.columns:
                col_canton = col
                break
        
        if col_canton is None:
            logging.warning("No se encontró columna de cantón. Usando ubicación genérica.")
            # Crear una ubicación por defecto
            dim_ubicacion = pd.DataFrame([{
                'ID_Ubicacion': 1,
                'CodigoCanton': '99999',
                'NombreCanton': 'NO_ESPECIFICADO',
                'Provincia': 'NO_ESPECIFICADA',
                'Region': 'NO_ESPECIFICADA',
                'Pais': 'ECUADOR'
            }])
        else:
            # Obtener cantones únicos del dataset
            cantones_dataset = df[col_canton].dropna().unique()
            
            # Crear dimensión ubicación
            ubicaciones = []
            id_counter = 1
            
            for codigo_canton in cantones_dataset:
                codigo_str = str(codigo_canton).strip()
                if codigo_str in mapeo_cantones:
                    info = mapeo_cantones[codigo_str]
                    ubicaciones.append({
                        'ID_Ubicacion': id_counter,
                        'CodigoCanton': codigo_str,
                        'NombreCanton': info['canton'],
                        'Provincia': info['provincia'],
                        'Region': info['region'],
                        'Pais': 'ECUADOR'
                    })
                else:
                    # Para cantones no mapeados, crear entrada genérica
                    ubicaciones.append({
                        'ID_Ubicacion': id_counter,
                        'CodigoCanton': codigo_str,
                        'NombreCanton': f'CANTON_{codigo_str}',
                        'Provincia': 'NO_IDENTIFICADA',
                        'Region': 'NO_IDENTIFICADA',
                        'Pais': 'ECUADOR'
                    })
                id_counter += 1
            
            dim_ubicacion = pd.DataFrame(ubicaciones)
        
        logging.info(f"🔧 Transformación completada: {len(dim_ubicacion)} ubicaciones únicas")
        
        # Cargar a BigQuery
        table_id = f'{PROJECT_ID}.{DATASET_ID}.dim_ubicacion'
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        job = bigquery_client.load_table_from_dataframe(dim_ubicacion, table_id, job_config=job_config)
        job.result()
        
        logging.info(f"✅ Cargados {len(dim_ubicacion)} registros en dim_ubicacion")
        return f"Dim_Ubicacion cargada exitosamente: {len(dim_ubicacion)} registros"
        
    except Exception as e:
        logging.error(f"❌ Error en ETL Dim_Ubicacion: {str(e)}")
        raise

# ===============================
# FUNCIÓN ETL PARA TABLA DE HECHOS
# ===============================

def etl_fact_registro_vehiculos(**context):
    """
    Proceso ETL para la tabla de hechos Fact_RegistroVehiculos
    Realiza lookups con las dimensiones y carga métricas
    """
    try:
        logging.info("📊 Iniciando ETL para Fact_RegistroVehiculos...")
        
        # Configurar clientes
        storage_client = storage.Client()
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        
        # Extraer datos principales del bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob('raw-data/sri_vehiculos.csv')
        
        content = blob.download_as_text()
        df_hechos = pd.read_csv(StringIO(content))
        
        logging.info(f"📊 Datos extraídos: {len(df_hechos)} registros de hechos")
        
        # Cargar dimensiones desde BigQuery para lookups
        logging.info("🔍 Cargando dimensiones para lookups...")
        
        try:
            # Cargar Dim_Tiempo
            query_tiempo = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dim_tiempo`"
            dim_tiempo = bigquery_client.query(query_tiempo).to_dataframe()
            
            # Cargar Dim_Vehiculo  
            query_vehiculo = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dim_vehiculo`"
            dim_vehiculo = bigquery_client.query(query_vehiculo).to_dataframe()
            
            # Cargar Dim_Transaccion
            query_transaccion = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dim_transaccion`"
            dim_transaccion = bigquery_client.query(query_transaccion).to_dataframe()
            
            # Cargar Dim_Ubicacion
            query_ubicacion = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dim_ubicacion`"
            dim_ubicacion = bigquery_client.query(query_ubicacion).to_dataframe()
            
            logging.info("✅ Dimensiones cargadas para lookups")
            
        except Exception as e:
            logging.error(f"Error cargando dimensiones: {str(e)}")
            raise
        
        # Procesamiento de fechas
        logging.info("📅 Procesando fechas...")
        
        # Buscar columna de fecha
        col_fecha = None
        for col in ['FECHA PROCESO', 'FECHA_PROCESO', 'fecha_proceso', 'FECHA']:
            if col in df_hechos.columns:
                col_fecha = col
                break
        
        if col_fecha:
            try:
                df_hechos['FECHA_PROCESO_CONV'] = pd.to_datetime(df_hechos[col_fecha], errors='coerce')
                # Filtrar fechas válidas
                df_hechos = df_hechos.dropna(subset=['FECHA_PROCESO_CONV'])
                df_hechos['FECHA_PROCESO_DATE'] = df_hechos['FECHA_PROCESO_CONV'].dt.date
            except Exception as e:
                logging.warning(f"Error procesando fechas: {str(e)}. Usando fecha por defecto.")
                df_hechos['FECHA_PROCESO_DATE'] = datetime.now().date()
        else:
            logging.warning("No se encontró columna de fecha. Usando fecha actual.")
            df_hechos['FECHA_PROCESO_DATE'] = datetime.now().date()
        
        # Realizar lookups con dimensiones
        logging.info("🔗 Realizando lookups con dimensiones...")
        
        # Lookup con Dim_Tiempo
        df_hechos = df_hechos.merge(
            dim_tiempo[['ID_Tiempo', 'FechaCompleta']], 
            left_on='FECHA_PROCESO_DATE', 
            right_on='FechaCompleta', 
            how='left'
        )
        
        # Lookup con Dim_Vehiculo (usando código de vehículo)
        col_codigo_vehiculo = None
        for col in ['CÓDIGO DE VEHÍCULO', 'CODIGO_VEHICULO', 'codigo_vehiculo']:
            if col in df_hechos.columns:
                col_codigo_vehiculo = col
                break
        
        if col_codigo_vehiculo:
            df_hechos = df_hechos.merge(
                dim_vehiculo[['ID_Vehiculo', 'CodigoVehiculo']], 
                left_on=col_codigo_vehiculo, 
                right_on='CodigoVehiculo', 
                how='left'
            )
        else:
            df_hechos['ID_Vehiculo'] = 1  # ID por defecto
        
        # Lookup con Dim_Transaccion
        merge_cols = []
        if 'TIPO TRANSACCIÓN' in df_hechos.columns and 'TipoTransaccion' in dim_transaccion.columns:
            merge_cols.append(('TIPO TRANSACCIÓN', 'TipoTransaccion'))
        if 'TIPO SERVICIO' in df_hechos.columns and 'TipoServicio' in dim_transaccion.columns:
            merge_cols.append(('TIPO SERVICIO', 'TipoServicio'))
        
        if merge_cols:
            left_cols = [col[0] for col in merge_cols]
            right_cols = [col[1] for col in merge_cols]
            df_hechos = df_hechos.merge(
                dim_transaccion[['ID_Transaccion'] + right_cols], 
                left_on=left_cols, 
                right_on=right_cols, 
                how='left'
            )
        else:
            df_hechos['ID_Transaccion'] = 1  # ID por defecto
        
        # Lookup con Dim_Ubicacion
        col_canton = None
        for col in ['CANTON', 'CANTÓN', 'canton']:
            if col in df_hechos.columns:
                col_canton = col
                break
        
        if col_canton:
            df_hechos[col_canton] = df_hechos[col_canton].astype(str)
            df_hechos = df_hechos.merge(
                dim_ubicacion[['ID_Ubicacion', 'CodigoCanton']], 
                left_on=col_canton, 
                right_on='CodigoCanton', 
                how='left'
            )
        else:
            df_hechos['ID_Ubicacion'] = 1  # ID por defecto
        
        # Crear tabla de hechos final
        logging.info("📋 Creando tabla de hechos final...")
        
        # Generar ID único para cada registro
        df_hechos['ID_Registro'] = range(1, len(df_hechos) + 1)
        
        # Calcular métricas
        df_hechos['CantidadRegistros'] = 1
        
        # Buscar columna de avalúo
        col_avaluo = None
        for col in ['AVALUO', 'AVALÚO', 'avaluo', 'avalúo']:
            if col in df_hechos.columns:
                col_avaluo = col
                break
        
        if col_avaluo:
            df_hechos['MontoAvaluo'] = pd.to_numeric(df_hechos[col_avaluo], errors='coerce').fillna(0)
        else:
            df_hechos['MontoAvaluo'] = 0
        
        # Seleccionar columnas finales para la tabla de hechos
        columnas_fact = [
            'ID_Registro',
            'ID_Tiempo',
            'ID_Vehiculo', 
            'ID_Transaccion',
            'ID_Ubicacion',
            'CantidadRegistros',
            'MontoAvaluo'
        ]
        
        # Verificar que todas las columnas existen
        columnas_existentes = [col for col in columnas_fact if col in df_hechos.columns]
        fact_table = df_hechos[columnas_existentes].copy()
        
        # Llenar valores nulos con defaults
        for col in ['ID_Tiempo', 'ID_Vehiculo', 'ID_Transaccion', 'ID_Ubicacion']:
            if col in fact_table.columns:
                fact_table[col] = fact_table[col].fillna(1)
        
        fact_table = fact_table.fillna(0)
        
        logging.info(f"🔧 Tabla de hechos creada: {len(fact_table)} registros")
        
        # Cargar a BigQuery
        table_id = f'{PROJECT_ID}.{DATASET_ID}.fact_registro_vehiculos'
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        job = bigquery_client.load_table_from_dataframe(fact_table, table_id, job_config=job_config)
        job.result()
        
        logging.info(f"✅ Cargados {len(fact_table)} registros en fact_registro_vehiculos")
        return f"Fact_RegistroVehiculos cargada exitosamente: {len(fact_table)} registros"
        
    except Exception as e:
        logging.error(f"❌ Error en ETL Fact_RegistroVehiculos: {str(e)}")
        raise

# ===============================
# DEFINICIÓN DE TAREAS DEL DAG
# ===============================

# Tarea de inicio
inicio = DummyOperator(
    task_id='inicio_proceso_etl',
    dag=dag
)

# Tareas ETL para dimensiones
tarea_dim_tiempo = PythonOperator(
    task_id='etl_dim_tiempo',
    python_callable=etl_dim_tiempo,
    dag=dag
)

tarea_dim_vehiculo = PythonOperator(
    task_id='etl_dim_vehiculo',
    python_callable=etl_dim_vehiculo,
    dag=dag
)

tarea_dim_transaccion = PythonOperator(
    task_id='etl_dim_transaccion',
    python_callable=etl_dim_transaccion,
    dag=dag
)

tarea_dim_ubicacion = PythonOperator(
    task_id='etl_dim_ubicacion',
    python_callable=etl_dim_ubicacion,
    dag=dag
)

# Tarea de sincronización para dimensiones
sincronizacion_dimensiones = DummyOperator(
    task_id='sincronizacion_dimensiones',
    dag=dag
)

# Tarea ETL para tabla de hechos
tarea_fact_registro = PythonOperator(
    task_id='etl_fact_registro_vehiculos',
    python_callable=etl_fact_registro_vehiculos,
    dag=dag
)

# Tarea de finalización
finalizacion = DummyOperator(
    task_id='finalizacion_proceso_etl',
    dag=dag
)

# ===============================
# FUNCIONES DE VALIDACIÓN Y MONITOREO
# ===============================

def validar_calidad_datos(**context):
    """
    Función para validar la calidad de los datos cargados
    """
    try:
        logging.info("🔍 Iniciando validación de calidad de datos...")
        
        client = bigquery.Client(project=PROJECT_ID)
        
        # Validaciones para dimensiones
        validaciones = []
        
        # Validar Dim_Tiempo
        query_tiempo = f"""
        SELECT 
            COUNT(*) as total_registros,
            COUNT(DISTINCT Anio) as anios_unicos,
            MIN(FechaCompleta) as fecha_min,
            MAX(FechaCompleta) as fecha_max
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_tiempo`
        """
        
        result_tiempo = client.query(query_tiempo).to_dataframe()
        validaciones.append(f"Dim_Tiempo: {result_tiempo.iloc[0]['total_registros']} registros, "
                          f"años {result_tiempo.iloc[0]['anios_unicos']}, "
                          f"rango: {result_tiempo.iloc[0]['fecha_min']} a {result_tiempo.iloc[0]['fecha_max']}")
        
        # Validar Dim_Vehiculo
        query_vehiculo = f"""
        SELECT 
            COUNT(*) as total_registros,
            COUNT(DISTINCT Marca) as marcas_unicas,
            COUNT(DISTINCT Clase) as clases_unicas
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_vehiculo`
        """
        
        result_vehiculo = client.query(query_vehiculo).to_dataframe()
        validaciones.append(f"Dim_Vehiculo: {result_vehiculo.iloc[0]['total_registros']} registros, "
                          f"{result_vehiculo.iloc[0]['marcas_unicas']} marcas, "
                          f"{result_vehiculo.iloc[0]['clases_unicas']} clases")
        
        # Validar Dim_Transaccion
        query_transaccion = f"""
        SELECT 
            COUNT(*) as total_registros,
            COUNT(DISTINCT TipoTransaccion) as tipos_transaccion
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_transaccion`
        """
        
        result_transaccion = client.query(query_transaccion).to_dataframe()
        validaciones.append(f"Dim_Transaccion: {result_transaccion.iloc[0]['total_registros']} registros, "
                          f"{result_transaccion.iloc[0]['tipos_transaccion']} tipos de transacción")
        
        # Validar Dim_Ubicacion
        query_ubicacion = f"""
        SELECT 
            COUNT(*) as total_registros,
            COUNT(DISTINCT Provincia) as provincias_unicas,
            COUNT(DISTINCT Region) as regiones_unicas
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_ubicacion`
        """
        
        result_ubicacion = client.query(query_ubicacion).to_dataframe()
        validaciones.append(f"Dim_Ubicacion: {result_ubicacion.iloc[0]['total_registros']} registros, "
                          f"{result_ubicacion.iloc[0]['provincias_unicas']} provincias, "
                          f"{result_ubicacion.iloc[0]['regiones_unicas']} regiones")
        
        # Validar Fact_RegistroVehiculos
        query_fact = f"""
        SELECT 
            COUNT(*) as total_registros,
            SUM(CantidadRegistros) as total_cantidad,
            AVG(MontoAvaluo) as avaluo_promedio,
            COUNT(CASE WHEN ID_Tiempo IS NULL THEN 1 END) as registros_sin_tiempo,
            COUNT(CASE WHEN ID_Vehiculo IS NULL THEN 1 END) as registros_sin_vehiculo
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_registro_vehiculos`
        """
        
        result_fact = client.query(query_fact).to_dataframe()
        validaciones.append(f"Fact_RegistroVehiculos: {result_fact.iloc[0]['total_registros']} registros, "
                          f"cantidad total: {result_fact.iloc[0]['total_cantidad']}, "
                          f"avalúo promedio: ${result_fact.iloc[0]['avaluo_promedio']:,.2f}")
        
        # Log de todas las validaciones
        for validacion in validaciones:
            logging.info(f"✅ {validacion}")
        
        # Verificar integridad referencial
        query_integridad = f"""
        SELECT 
            COUNT(*) as registros_con_claves_validas
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_registro_vehiculos` f
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_tiempo` t ON f.ID_Tiempo = t.ID_Tiempo
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_vehiculo` v ON f.ID_Vehiculo = v.ID_Vehiculo
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_transaccion` tr ON f.ID_Transaccion = tr.ID_Transaccion
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_ubicacion` u ON f.ID_Ubicacion = u.ID_Ubicacion
        """
        
        result_integridad = client.query(query_integridad).to_dataframe()
        registros_validos = result_integridad.iloc[0]['registros_con_claves_validas']
        
        logging.info(f"🔗 Integridad referencial: {registros_validos} registros con todas las claves válidas")
        
        resumen_validacion = {
            'validaciones': validaciones,
            'registros_con_integridad': registros_validos,
            'timestamp': datetime.now().isoformat()
        }
        
        return resumen_validacion
        
    except Exception as e:
        logging.error(f"❌ Error en validación de calidad: {str(e)}")
        raise

def generar_metricas_negocio(**context):
    """
    Genera métricas de negocio del proceso ETL
    """
    try:
        logging.info("📈 Generando métricas de negocio...")
        
        client = bigquery.Client(project=PROJECT_ID)
        
        # Métricas por año
        query_por_anio = f"""
        SELECT 
            t.Anio,
            COUNT(*) as total_registros,
            SUM(f.MontoAvaluo) as monto_total_avaluo,
            AVG(f.MontoAvaluo) as monto_promedio_avaluo
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_registro_vehiculos` f
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_tiempo` t ON f.ID_Tiempo = t.ID_Tiempo
        GROUP BY t.Anio
        ORDER BY t.Anio DESC
        LIMIT 5
        """
        
        metricas_anio = client.query(query_por_anio).to_dataframe()
        
        # Métricas por marca
        query_por_marca = f"""
        SELECT 
            v.Marca,
            COUNT(*) as total_registros,
            AVG(f.MontoAvaluo) as avaluo_promedio
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_registro_vehiculos` f
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_vehiculo` v ON f.ID_Vehiculo = v.ID_Vehiculo
        GROUP BY v.Marca
        ORDER BY total_registros DESC
        LIMIT 10
        """
        
        metricas_marca = client.query(query_por_marca).to_dataframe()
        
        # Métricas por provincia
        query_por_provincia = f"""
        SELECT 
            u.Provincia,
            u.Region,
            COUNT(*) as total_registros,
            SUM(f.MontoAvaluo) as monto_total
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_registro_vehiculos` f
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_ubicacion` u ON f.ID_Ubicacion = u.ID_Ubicacion
        GROUP BY u.Provincia, u.Region
        ORDER BY total_registros DESC
        LIMIT 10
        """
        
        metricas_provincia = client.query(query_por_provincia).to_dataframe()
        
        # Log de métricas
        logging.info("📊 MÉTRICAS POR AÑO:")
        for _, row in metricas_anio.iterrows():
            logging.info(f"   {row['Anio']}: {row['total_registros']} registros, "
                        f"avalúo total: ${row['monto_total_avaluo']:,.2f}")
        
        logging.info("🚗 TOP MARCAS:")
        for _, row in metricas_marca.iterrows():
            logging.info(f"   {row['Marca']}: {row['total_registros']} registros, "
                        f"avalúo promedio: ${row['avaluo_promedio']:,.2f}")
        
        logging.info("🌎 TOP PROVINCIAS:")
        for _, row in metricas_provincia.iterrows():
            logging.info(f"   {row['Provincia']} ({row['Region']}): {row['total_registros']} registros")
        
        metricas_resumen = {
            'metricas_por_anio': metricas_anio.to_dict('records'),
            'metricas_por_marca': metricas_marca.to_dict('records'),
            'metricas_por_provincia': metricas_provincia.to_dict('records'),
            'timestamp': datetime.now().isoformat()
        }
        
        return metricas_resumen
        
    except Exception as e:
        logging.error(f"❌ Error generando métricas: {str(e)}")
        raise

def notificar_finalizacion(**context):
    """
    Notifica la finalización exitosa del proceso ETL
    """
    try:
        logging.info("📧 Enviando notificación de finalización...")
        
        # Obtener información del contexto
        dag_run = context['dag_run']
        execution_date = context['execution_date']
        
        # Crear resumen del proceso
        resumen = {
            'dag_id': dag_run.dag_id,
            'execution_date': execution_date.isoformat(),
            'estado': 'EXITOSO',
            'duracion_total': str(datetime.now() - dag_run.start_date) if dag_run.start_date else 'N/A',
            'timestamp_finalizacion': datetime.now().isoformat()
        }
        
        logging.info("✅ PROCESO ETL FINALIZADO EXITOSAMENTE")
        logging.info(f"   DAG: {resumen['dag_id']}")
        logging.info(f"   Fecha de ejecución: {resumen['execution_date']}")
        logging.info(f"   Duración: {resumen['duracion_total']}")
        logging.info(f"   Estado: {resumen['estado']}")
        
        # Aquí se puede agregar lógica para enviar emails, Slack, etc.
        # Por ejemplo:
        # send_email_notification(resumen)
        # send_slack_notification(resumen)
        
        return resumen
        
    except Exception as e:
        logging.error(f"❌ Error en notificación: {str(e)}")
        raise

# ===============================
# TAREAS DE VALIDACIÓN Y MONITOREO
# ===============================

tarea_validacion = PythonOperator(
    task_id='validar_calidad_datos',
    python_callable=validar_calidad_datos,
    dag=dag
)

tarea_metricas = PythonOperator(
    task_id='generar_metricas_negocio',
    python_callable=generar_metricas_negocio,
    dag=dag
)

tarea_notificacion = PythonOperator(
    task_id='notificar_finalizacion',
    python_callable=notificar_finalizacion,
    dag=dag
)

# ===============================
# DEFINICIÓN DE DEPENDENCIAS DEL DAG
# ===============================

# Estructura de dependencias:
# inicio -> [dimensiones en paralelo] -> sincronización -> tabla_hechos -> validación -> métricas -> notificación -> fin

# Inicio del proceso
inicio >> [tarea_dim_tiempo, tarea_dim_vehiculo, tarea_dim_transaccion, tarea_dim_ubicacion]

# Sincronización de dimensiones
[tarea_dim_tiempo, tarea_dim_vehiculo, tarea_dim_transaccion, tarea_dim_ubicacion] >> sincronizacion_dimensiones

# Tabla de hechos (después de que todas las dimensiones estén listas)
sincronizacion_dimensiones >> tarea_fact_registro

# Validación y métricas
tarea_fact_registro >> tarea_validacion >> tarea_metricas >> tarea_notificacion >> finalizacion

# ===============================
# CONFIGURACIÓN ADICIONAL DEL DAG
# ===============================

# Configurar el DAG para logging detallado
dag.doc_md = """
# DAG ETL SRI Vehículos

Este DAG implementa un proceso ETL completo para los datos vehiculares del SRI.

## Estructura del Proceso:

1. **Dimensiones (Paralelo)**:
   - `dim_tiempo`: Genera calendario completo 2020-2025
   - `dim_vehiculo`: Extrae características únicas de vehículos
   - `dim_transaccion`: Mapea tipos de transacciones
   - `dim_ubicacion`: Mapea códigos de cantón a geografía

2. **Tabla de Hechos**:
   - `fact_registro_vehiculos`: Combina todas las dimensiones con métricas

3. **Validación y Monitoreo**:
   - Validación de calidad de datos
   - Generación de métricas de negocio
   - Notificaciones de finalización

## Configuración Requerida:

- PROJECT_ID: ID del proyecto de Google Cloud
- DATASET_ID: Nombre del dataset en BigQuery
- BUCKET_NAME: Nombre del bucket de Cloud Storage
- Service Account con permisos de BigQuery y Cloud Storage

## Archivos Requeridos:

- `gs://[BUCKET_NAME]/raw-data/sri_vehiculos.csv`

## Tablas Generadas:

- `dim_tiempo`
- `dim_vehiculo` 
- `dim_transaccion`
- `dim_ubicacion`
- `fact_registro_vehiculos`
"""

# Configurar tags adicionales para organización
dag.tags.extend(['data-warehouse', 'gobierno', 'vehiculos'])

if __name__ == "__main__":
    dag.test()
