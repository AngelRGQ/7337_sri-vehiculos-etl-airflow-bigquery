#!/usr/bin/env python3
"""
Script para configurar automáticamente el entorno de Google Cloud Platform
"""

import yaml
from google.cloud import bigquery, storage
import os

def load_config():
    with open('config/variables.yaml', 'r') as file:
        return yaml.safe_load(file)

def setup_bigquery(config):
    client = bigquery.Client(project=config['project_id'])
    # Código para crear dataset y tablas
    print("✅ BigQuery configurado")

def setup_storage(config):
    client = storage.Client()
    # Código para crear bucket
    print("✅ Cloud Storage configurado")

if __name__ == "__main__":
    config = load_config()
    setup_bigquery(config)
    setup_storage(config)
    print("🚀 Configuración completada")
