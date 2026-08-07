"""
===============================================================================
PLANTILLA BASE PARA EL ESTUDIANTE: TALLER DE ETL CON GENERADORES (YIELD) - CLÍNICA
===============================================================================
Materia: Gestión de Datos / ETL Avanzado
Instrucciones: Completa los bloques señalados con '# TODO:' para implementar un
               pipeline de ETL capaz de procesar archivos masivos de admisiones
               médicas mediante streaming (generadores en Python con yield) sin
               agotar la memoria RAM de los servidores de la Clínica San José.
===============================================================================
"""

import csv
import os
import time
import tracemalloc
import sqlite3
from getpass import getpass

# Intentar importar psycopg2 para PostgreSQL. Si el estudiante no lo tiene instalado o no tiene Postgres corriendo,
# se incluye soporte alternativo automático con SQLite para pruebas locales.
try:
    from mysql import connector
    MYSQL_DISPONIBLE = True
except ImportError:
    connector = None
    MYSQL_DISPONIBLE = False


# =============================================================================
# FASE 1: EXTRACT - GENERADOR CON YIELD (STREAMING / LAZY EVALUATION)
# =============================================================================
def extractor_lotes_csv(ruta_csv, tamano_lote=2000):
    """
    Función Generadora que lee un archivo CSV gigante de admisiones de forma perezosa (Lazy)
    usando la instrucción 'yield'. En lugar de cargar todo el archivo en RAM,
    retorna un lote (chunk) de registros a la vez.

    Parámetros:
        ruta_csv (str): Ruta al archivo CSV.
        tamano_lote (int): Cantidad de filas por cada lote producido.

    Yields:
        list[dict]: Lista de diccionarios representando las filas de un lote.
    """
    # TODO: COMPLETAR POR EL ESTUDIANTE
    # 1. Abrir el archivo CSV en modo lectura ('r') con encoding='utf-8'.
    
    # 2. Utilizar csv.DictReader(f) para leer las filas como diccionarios.
    # 3. Acumular las filas en una lista llamada 'lote'.
    # 4. Cuando len(lote) == tamano_lote, emitir el lote usando: yield lote
    # 5. Reiniciar la lista 'lote = []' para la siguiente iteración.
    # 6. Al salir del bucle, si quedan elementos remanentes en 'lote', emitirlos con 'yield lote'.
                
    if not os.path.exists(ruta_csv):
        raise FileNotFoundError(f"No existe el archivo: {ruta_csv}")

    lote = []

    with open(ruta_csv, mode="r", encoding="utf-8", newline="") as archivo:
        lector = csv.DictReader(archivo)

        for fila in lector:
            lote.append(fila)

            if len(lote) == tamano_lote:
                yield lote
                lote = []

        # Entregar el último lote si tiene menos de 2000 filas
        if lote:
            yield lote

# =============================================================================
# FASE 2: TRANSFORM - REGLAS DE NEGOCIO Y LIMPIEZA LOTE A LOTE (CLÍNICA)
# =============================================================================
def transformar_lote(lote_raw):
    """
    Transforma un lote de registros crudos aplicando reglas de limpieza
    y cálculos clínicos de la clínica San José.

    Reglas de Negocio:
    1. Descartar registros con costo_consulta vacío o menor/igual a 0 (Limpieza).
    2. Calcular Comisión de Seguro del 5% sobre el costo_consulta (procesamiento administrativo).
    3. Calcular Costo Neto = costo_consulta - comision_seguro.
    4. Marcar bandera 'alerta_gravedad' = True si el costo > 200.00 y el estado del paciente es 'Critico' o 'Grave'.

    Returns:
        list[tuple]: Lista de tuplas estructuradas para inserción SQL en lote.
    """
    lote_transformado = []

    for registro in lote_raw:
        try:
            costo_texto = registro.get("costo_consulta", "").strip()

            # Regla 1: eliminar costos vacíos
            if not costo_texto:
                continue

            costo = float(costo_texto)

            # Regla 1: eliminar costos menores o iguales a cero
            if costo <= 0:
                continue

            # Regla 2: comisión administrativa del seguro
            comision_seguro = round(costo * 0.05, 2)

            # Regla 3: costo neto
            costo_neto = round(costo - comision_seguro, 2)

            estado = registro.get("estado_paciente", "").strip()

            # Regla 4: alerta de gravedad
            alerta_gravedad = (
                costo > 200.00
                and estado in ("Grave", "Critico")
            )

            fila_transformada = (
                registro["id_admision"],
                registro["fecha_ingreso"],
                registro["id_paciente"],
                registro["cama_asignada"],
                registro["diagnostico"],
                costo,
                comision_seguro,
                costo_neto,
                alerta_gravedad,
                estado
            )

            lote_transformado.append(fila_transformada)

        except (ValueError, TypeError, KeyError):
            # Si la fila está dañada o le faltan columnas, se descarta
            continue

    return lote_transformado

# =============================================================================
# FASE 3: LOAD - CARGA EN LOTE (BATCH LOAD) EN BASE DE DATOS
# =============================================================================
def cargar_lote_sqlite(conn, lote_transformado):
    """
    Carga un lote de registros transformados en la base de datos SQLite.
    """
    if not lote_transformado:
        return

    sql = """
        INSERT INTO admisiones_emergencia (
            id_admision,
            fecha_ingreso,
            id_paciente,
            cama_asignada,
            diagnostico,
            costo_consulta,
            comision_seguro,
            costo_neto,
            alerta_gravedad,
            estado_paciente
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    cursor = conn.cursor()
    cursor.executemany(sql, lote_transformado)
    conn.commit()

def cargar_lote_mysql(conn, lote_transformado):
    """
    Carga un lote de registros transformados en PostgreSQL utilizando execute_values o executemany.
    """
    # TODO: SI USAN POSTGRESQL, IMPLEMENTAR AQUÍ CON PSYCOPG2
    if not lote_transformado:
        return

    sql = """
        INSERT INTO admisiones_emergencia (
            id_admision,
            fecha_ingreso,
            id_paciente,
            cama_asignada,
            diagnostico,
            costo_consulta,
            comision_seguro,
            costo_neto,
            alerta_gravedad,
            estado_paciente
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    cursor = conn.cursor()

    try:
        cursor.executemany(sql, lote_transformado)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()

# =============================================================================
# EJECUCIÓN PRINCIPAL Y MONITOREO DE MEMORIA RAM
# =============================================================================
def ejecutar_pipeline():
    directorio_base = os.path.dirname(os.path.abspath(__file__))

    ruta_csv = os.path.join(
        directorio_base,
        "logs_admisiones_masivas.csv"
    )

    print("=" * 70)
    print(" INICIANDO PIPELINE ETL CON GENERADORES - CLÍNICA SAN JOSÉ ")
    print("=" * 70)

    # Verificar que el conector de MySQL esté instalado
    if not MYSQL_DISPONIBLE:
        raise RuntimeError(
            "No está instalado mysql-connector-python. "
            "Ejecuta: python3 -m pip install mysql-connector-python"
        )

    # Conexión con MySQL
    conn = connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password=getpass("Escribe tu contraseña de MySQL: "),
        database="clinica_san_jose",
        charset="utf8mb4"
    )

    print("-> Conexión con MySQL establecida correctamente.")

    # Iniciar medición de RAM y tiempo
    tracemalloc.start()
    tiempo_inicio = time.time()

    total_procesados = 0
    total_lotes = 0

    print("-> Procesando archivo masivo en lotes usando yield...")

    try:
        # EXTRACT: obtener un lote de 2000 registros
        for lote_raw in extractor_lotes_csv(
            ruta_csv,
            tamano_lote=2000
        ):
            total_lotes += 1

            # TRANSFORM: limpiar y calcular valores
            lote_transformado = transformar_lote(lote_raw)

            # LOAD: insertar el lote en MySQL
            cargar_lote_mysql(conn, lote_transformado)

            total_procesados += len(lote_transformado)

            # Obtener memoria pico del proceso
            memoria_actual, memoria_pico = (
                tracemalloc.get_traced_memory()
            )

            memoria_pico_mb = memoria_pico / (1024 * 1024)

            print(
                f"Lote #{total_lotes}: "
                f"{len(lote_transformado):,} filas cargadas | "
                f"Total: {total_procesados:,} | "
                f"RAM pico: {memoria_pico_mb:.2f} MB"
            )

    finally:
        # Cerrar la conexión aunque ocurra un error
        if conn.is_connected():
            conn.close()
            print("-> Conexión con MySQL cerrada.")

    # Calcular resultados finales
    duracion = time.time() - tiempo_inicio

    memoria_actual, memoria_pico = tracemalloc.get_traced_memory()
    memoria_pico_mb = memoria_pico / (1024 * 1024)

    tracemalloc.stop()

    print("\n" + "=" * 70)
    print(" RESUMEN DEL PIPELINE ETL")
    print("=" * 70)
    print(f"Filas cargadas:    {total_procesados:,}")
    print(f"Lotes procesados:  {total_lotes}")
    print(f"Tiempo:            {duracion:.2f} segundos")
    print(f"RAM pico:          {memoria_pico_mb:.2f} MB")

    if memoria_pico_mb <= 20:
        print("[ÉXITO] El pipeline respetó el límite de 20 MB.")
    else:
        print("[ADVERTENCIA] El pipeline superó el límite de 20 MB.")


if __name__ == "__main__":
    ejecutar_pipeline()
