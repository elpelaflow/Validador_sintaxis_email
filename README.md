# Validador_sintaxis_email

Este proyecto permite importar archivos CSV a SQL Server mediante una interfaz gráfica.

Si la aplicación encuentra algún problema, los detalles se registran en `logs/app.log`.
Revisa ese archivo para obtener información técnica sobre el error.

## Configuración adicional

Puedes ajustar la variable `max_emails` en `importador_sql.py` para limitar la cantidad de correos que se almacenan por casilla. Usa `None` para no establecer límites.