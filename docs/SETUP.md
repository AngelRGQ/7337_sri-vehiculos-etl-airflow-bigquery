# 🚀 Guía de Instalación Detallada

## Prerrequisitos

### Software Requerido
- Python 3.8 o superior
- Git
- Cuenta de Google Cloud Platform (con facturación habilitada)

### Conocimientos Recomendados
- Conceptos básicos de ETL
- SQL intermedio
- Python básico
- Conocimientos básicos de Google Cloud Platform

## Paso 1: Configuración de Google Cloud Platform

### 1.1 Crear Proyecto en GCP
```bash
# Instalar Google Cloud CLI
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Autenticarse
gcloud auth login

# Crear proyecto
gcloud projects create sri-vehiculos-etl-[TU-ID-UNICO] --name="SRI Vehículos ETL"

# Configurar proyecto activo
gcloud config set project sri-vehiculos-etl-[TU-ID-UNICO]
```

### 1.2 Habilitar APIs Necesarias
```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable storage-api.googleapis.com
gcloud services enable storage.googleapis.com
```

### 1.3 Crear Service Account
```bash
# Crear service account
gcloud iam service-accounts create sri-vehiculos-etl-sa \
    --description="Service Account para ETL de vehículos SRI" \
    --display-name="SRI Vehículos ETL SA"

# Asignar roles
gcloud projects add-iam-policy-binding sri-vehiculos-etl-[TU-ID-UNICO] \
    --member="serviceAccount:sri-vehiculos-etl-sa@sri-vehiculos-etl-[TU-ID-UNICO].iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding sri-vehiculos-etl-[TU-ID-UNICO] \
    --member="serviceAccount:sri-vehiculos-etl-sa@sri-vehiculos-etl-[TU-ID-UNICO].iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Crear y descargar clave JSON
gcloud iam service-accounts keys create ~/sri-vehiculos-etl-key.json \
    --iam-account=sri-vehiculos-etl-sa@sri-vehiculos-etl-[TU-ID-UNICO].iam.gserviceaccount.com
```

## Paso 2: Configuración del Entorno Local

### 2.1 Clonar el Repositorio
```bash
git clone https://github.com/tu-usuario/sri-vehiculos-etl.git
cd sri-vehiculos-etl
```

### 2.2 Crear Entorno Virtual
```bash
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
# En Windows:
venv\Scripts\activate
# En macOS/Linux:
source venv/bin/activate
```

### 2.3 Instalar Dependencias
```bash
pip install -r requirements.txt
```

### 2.4 Configurar Variables de Entorno
```bash
# Crear archivo de configuración
cp config/variables.yaml.template config/variables.yaml

# Configurar variable de entorno para credenciales
export GOOGLE_APPLICATION_CREDENTIALS="~/sri-vehiculos-etl-key.json"

# En Windows:
set GOOGLE_APPLICATION_CREDENTIALS=C:\ruta\a\tu\sri-vehiculos-etl-key.json
```

### 2.5 Editar Configuración
Editar `config/variables.yaml` con tus valores:

```yaml
# config/variables.yaml
project_id: "sri-vehiculos-etl-[TU-ID-UNICO]"
dataset_id: "sri_vehiculos_dw"
bucket_name: "sri-vehiculos-etl-bucket-[TU-ID-UNICO]"
location: "US"  # o "EU" según tu preferencia
```

## Paso 3: Configuración de Apache Airflow

### 3.1 Inicializar Airflow
```bash
# Configurar directorio de Airflow
export AIRFLOW_HOME=~/airflow

# Inicializar base de datos
airflow db init

# Crear usuario administrador
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### 3.2 Configurar Conexiones en Airflow
```bash
# Crear conexión para Google Cloud
airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{"key_path": "~/sri-vehiculos-etl-key.json", "scope": "https://www.googleapis.com/auth/cloud-platform", "project": "sri-vehiculos-etl-[TU-ID-UNICO]"}'
```

### 3.3 Copiar DAG
```bash
# Copiar archivo DAG al directorio de Airflow
cp dags/sri_vehiculos_etl_dag.py ~/airflow/dags/
```

## Paso 4: Configuración de BigQuery y Cloud Storage

### 4.1 Ejecutar Script de Setup
```bash
python scripts/setup_gcp.py
```

Este script:
- Crea el dataset en BigQuery
- Crea el bucket en Cloud Storage
- Crea las tablas del modelo dimensional
- Verifica las conexiones

## Paso 5: Verificación de la Instalación

### 5.1 Verificar Conexiones
```bash
python scripts/validate_setup.py
```

### 5.2 Iniciar Airflow
```bash
# Terminal 1: Webserver
airflow webserver --port 8080

# Terminal 2: Scheduler
airflow scheduler
```

### 5.3 Acceder a la Interfaz Web
1. Abrir navegador en `http://localhost:8080`
2. Login con usuario: `admin`, password: `admin`
3. Verificar que el DAG `sri_vehiculos_etl` aparezca en la lista

## Paso 6: Prueba del Sistema

### 6.1 Subir Datos de Prueba
```bash
# Subir archivo CSV de prueba
gsutil cp data/sample_data.csv gs://sri-vehiculos-etl-bucket-[TU-ID-UNICO]/raw-data/sri_vehiculos.csv
```

### 6.2 Ejecutar DAG Manual
1. En la interfaz de Airflow, activar el DAG
2. Hacer clic en "Trigger DAG"
3. Monitorear la ejecución en la vista de Graph

### 6.3 Verificar Resultados
```bash
# Verificar tablas en BigQuery
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`sri-vehiculos-etl-[TU-ID-UNICO].sri_vehiculos_dw.fact_registro_vehiculos\`"
```

## Solución de Problemas Comunes

### Error: "No module named 'airflow'"
```bash
# Verificar que el entorno virtual esté activado
source venv/bin/activate
pip install apache-airflow==2.7.0
```

### Error: "Permission denied" en BigQuery
```bash
# Verificar roles del service account
gcloud projects get-iam-policy sri-vehiculos-etl-[TU-ID-UNICO]
```

### Error: "Bucket already exists"
```bash
# Usar un nombre de bucket único
gsutil mb gs://sri-vehiculos-etl-bucket-[TU-ID-UNICO]-[TIMESTAMP]
```

### Error: "DAG not found"
```bash
# Verificar que el archivo DAG esté en el directorio correcto
ls ~/airflow/dags/
# Reiniciar Airflow scheduler
```

## Opciones de Despliegue

### Opción 1: Google Colab (Más Fácil)
1. Usar el notebook `notebooks/SRI_Vehiculos_ETL_Colab.ipynb`
2. No requiere instalación local
3. Ideal para pruebas y demos

### Opción 2: Google Cloud Composer
1. Servicio Airflow gestionado en GCP
2. Más robusto para producción
3. Costos adicionales

### Opción 3: Docker
```bash
# Usar docker-compose (archivo incluido)
docker-compose up -d
```

## Próximos Pasos

1. ✅ Completar instalación
2. ✅ Ejecutar prueba con datos de muestra
3. 📊 Cargar datos reales del SRI
4. 📈 Crear dashboards en Data Studio/Looker
5. 🔄 Programar ejecución automática

## Soporte

Si encuentras problemas:
1. Revisar logs en `~/airflow/logs/`
2. Consultar la documentación de [Apache Airflow](https://airflow.apache.org/docs/)
3. Revisar la documentación de [Google Cloud](https://cloud.google.com/docs)
4. Crear un issue en el repositorio GitHub

---

**¡Felicitaciones! Tu entorno ETL está listo para procesar datos del SRI 🚀**
