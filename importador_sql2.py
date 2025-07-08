import os
import pandas as pd
import pyodbc
from datetime import datetime
from collections import defaultdict
from tkinter import Tk, filedialog, messagebox
import re
import logging

# CONFIGURACIÓN PRINCIPAL
database_name = "LeadsDB"
server = r"(local)\SQLEXPRESS"

# FUNCIÓN: Forzar todos los tipos a NVARCHAR(MAX)
def infer_sql_type(serie):
    return "NVARCHAR(MAX)"

# Crear carpeta de logs si no existe
logs_folder = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_folder, exist_ok=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(logs_folder, "app.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# FUNCION PARA EXTRAER EMAILS VALIDOS DE UN TEXTO
def extraer_emails_validos(texto):
    """Devuelve las direcciones de correo válidas encontradas en el texto.

    Si no se encuentra ninguna coincidencia, se devuelve el texto original.
    Se eliminan aquellos correos que terminan con extensiones de imagen.
    """

    if not isinstance(texto, str):
        texto = str(texto)

    patron = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    encontrados = re.findall(patron, texto)

    if encontrados:
        # Descartar mails que finalicen con extensiones de imágenes
        encontrados = [m for m in encontrados if not re.search(r"\.(png|jpe?g|gif|bmp|svg)$", m, re.IGNORECASE)]
        if encontrados:
            return ",".join(encontrados)

    return texto

# FUNCIÓN PRINCIPAL
def importar_archivo_csv(csv_path):
    table_name = os.path.splitext(os.path.basename(csv_path))[0]
    logging.info("Iniciando importaci\u00f3n de %s", csv_path)

    # ✅ Lectura segura del CSV
    df = pd.read_csv(
        csv_path,
        encoding="utf-8",
        delimiter=",",
        quotechar='"',
        skip_blank_lines=True
    )

    # ✅ Conversión obligatoria a texto para evitar errores de tipo
    
    df = df.astype(str).fillna("")
    
    # Limpiar posibles columnas de email
    for col in df.columns:
        if "mail" in col.lower():
            df[col] = df[col].apply(extraer_emails_validos)

    # CONECTAR A SQL SERVER
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database_name};"
        f"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Crear tabla si no existe
    columns_sql = []
    for col in df.columns:
        sql_type = infer_sql_type(df[col])
        col_clean = col.replace(" ", "_").replace("-", "_")
        columns_sql.append(f"[{col_clean}] {sql_type}")
    columns_clause = ",\n    ".join(columns_sql)

    create_sql = f"""
    IF OBJECT_ID(N'dbo.{table_name}', N'U') IS NULL
    BEGIN
        CREATE TABLE [dbo].[{table_name}] (
            {columns_clause}
        )
    END
    """
    cursor.execute(create_sql)
    conn.commit()

    # Inicializar logs
    errores = 0
    error_por_columna = defaultdict(int)
    causas_comunes = defaultdict(int)

    log_file_path = os.path.join(logs_folder, f"{table_name}_errores.log")
    informe_file_path = os.path.join(logs_folder, f"{table_name}_informe.txt")
    log_file = open(log_file_path, "w", encoding="utf-8")

    # Preparar inserción
    clean_cols = [c.replace(" ", "_").replace("-", "_") for c in df.columns]
    insert_sql = f"INSERT INTO [dbo].[{table_name}] ({','.join(f'[{c}]' for c in clean_cols)}) VALUES ({','.join('?' for _ in df.columns)})"

    # Insertar filas
    for idx, row in enumerate(df.itertuples(index=False, name=None), start=1):
        try:
            cursor.execute(insert_sql, row)
        except Exception as e:
            errores += 1
            log_file.write(f"Fila {idx} → {e}\n")
            logging.exception("Error al insertar fila %s", idx)

            # Inferir columna y tipo de error
            err_text = str(e).lower()
            if "float" in err_text:
                causas_comunes["❌ Valor no convertible a FLOAT"] += 1
            elif "int too big" in err_text:
                causas_comunes["❌ Entero demasiado grande"] += 1
            elif "nvarchar" in err_text or "string" in err_text:
                causas_comunes["❌ Texto demasiado largo"] += 1
            elif "could not convert" in err_text:
                causas_comunes["❌ Error de conversión de tipo"] += 1
            else:
                causas_comunes["❌ Otro tipo de error"] += 1

            # Estimar columna afectada si es posible
            for i, val in enumerate(row):
                if isinstance(val, float) and ("float" in err_text or "convert" in err_text):
                    col = clean_cols[i]
                    error_por_columna[col] += 1
                    break
                if isinstance(val, int) and "int too big" in err_text:
                    col = clean_cols[i]
                    error_por_columna[col] += 1
                    break

    conn.commit()
    log_file.close()
    cursor.close()
    conn.close()

    logging.info(
        "Finalizada importaci\u00f3n de %s: %d filas procesadas, %d errores",
        csv_path,
        len(df),
        errores,
    )

    # Crear informe descriptivo
    with open(informe_file_path, "w", encoding="utf-8") as f:
        f.write(f"📄 Resumen de errores durante la importación de {os.path.basename(csv_path)}\n\n")
        f.write(f"Total de filas procesadas: {len(df)}\n")
        f.write(f"Errores detectados: {errores}\n\n")

        if errores:
            f.write("🔎 Errores por columna:\n")
            for col, count in error_por_columna.items():
                f.write(f"  - {col}: {count} errores\n")
            f.write("\n📌 Causas comunes detectadas:\n")
            for causa, count in causas_comunes.items():
                f.write(f"  - {causa}: {count} veces\n")
        else:
            f.write("✅ No se detectaron errores.\n")

    return len(df), errores, log_file_path, informe_file_path

# UI
def seleccionar_csv():
    file_path = filedialog.askopenfilename(
        title="Seleccionar archivo CSV",
        filetypes=[("Archivos CSV", "*.csv")]
    )
    if not file_path:
        return

    try:
        logging.info("Archivo seleccionado: %s", file_path)
        total, errores, log, informe = importar_archivo_csv(file_path)
        msg = f"✅ {total - errores} filas importadas.\n❌ {errores} errores."
        if errores:
            msg += f"\nLog técnico: {log}\nResumen: {informe}"
        messagebox.showinfo("Importación finalizada", msg)
    except Exception as e:
        logging.exception("Error durante la importaci\u00f3n")
        messagebox.showerror("Error durante la importación", str(e))

# Lanzar UI
root = Tk()
root.title("Importador de Leads a SQL Server")
root.geometry("400x200")
root.resizable(False, False)

from tkinter import Button, Label
Label(root, text="Seleccioná un archivo CSV para importar", font=("Arial", 12)).pack(pady=30)
Button(root, text="Seleccionar archivo", command=seleccionar_csv, width=30).pack(pady=10)
Label(root, text="© El Pela Flow").pack(side="bottom", pady=10)

logging.info("Aplicaci\u00f3n iniciada")
root.mainloop()
