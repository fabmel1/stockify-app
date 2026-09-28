import os
from databricks.sdk import WorkspaceClient

# Imprimimos para verificar que Python las lee desde el entorno
print("HOST en Python:", os.getenv("DATABRICKS_HOST"))
print("TOKEN en Python:", "Configurado (oculto por seguridad)" if os.getenv("DATABRICKS_TOKEN") else "NO ENCONTRADO")

try:
    # Intentamos listar los clusters o un comando simple para probar autenticación
    w = WorkspaceClient()
    print("Intentando conectar con Databricks...")
    
    # Listar los primeros repositorios o volúmenes como prueba de fuego de autenticación
    # O simplemente consultar el estado actual del workspace
    current_user = w.current_user.me()
    print(f"¡Conexión exitosa! Autenticado como: {current_user.user_name}")

except Exception as e:
    print(f"❌ Falló la autenticación: {e}")