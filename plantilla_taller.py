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

# Intentar importar psycopg2 para PostgreSQL. Si el estudiante no lo tiene instalado o no tiene Postgres corriendo,
# se incluye soporte alternativo automático con SQLite para pruebas locales.
try:
    import psycopg2
    from psycopg2.extras import execute_values
    POSTGRES_DISPONIBLE = True
except ImportError:
    POSTGRES_DISPONIBLE = False


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
    dir_padre = os.path.dirname(ruta_csv)
    if dir_padre and not os.path.exists(dir_padre):
           os.makedirs(dir_padre)
   
    with open(ruta_csv, "w", encoding="utf-8") as f:
        for i in range(1, 101):
                registro = {
                   "id": i,
                   "nombre": f"Estudiante {i}",
                   "nota": round(7.0 + (i % 4) * 0.75, 2),
                   "materia": "Gestión de Datos"
                }
                f.write(json.dumps(registro, ensure_ascii=False) + "\n")
                            
        print(f"[OK] Archivo '{ruta_csv}' creado con 100 registros en formato JSONL.\n")
                
                
    
    if not os.path.exists(ruta_csv):
        raise FileNotFoundError(f"No existe el archivo: {ruta_csv}")

    with open(ruta_csv, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        lote = []
        for fila in reader:
            lote.append(fila)
            if len(lote) >= tamano_lote:
                yield lote
                lote = []
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

    for reg in lote_raw:
        # TODO: COMPLETAR LA LÓGICA DE TRANSFORMACIÓN POR EL ESTUDIANTE
        try:
            costo_str = reg.get("costo_consulta", "")
            if not costo_str:
                continue  # Descartar nulos

            costo = float(costo_str)
            if costo <= 0:
                continue  # Descartar costos inválidos/negativos

            # Cálculos de clínica
            comision = round(costo * 0.05, 2)
            costo_neto = round(costo - comision, 2)
            
            estado = reg.get("estado_paciente", "Leve")
            alerta_gravedad = (costo > 200.0) and (estado in ["Critico", "Grave"])

            tupla_registro = (
                reg["id_admision"],
                reg["fecha_ingreso"],
                reg["id_paciente"],
                reg["cama_asignada"],
                reg["diagnostico"],
                costo,
                comision,
                costo_neto,
                alerta_gravedad,
                estado
            )
            lote_transformado.append(tupla_registro)

        except (ValueError, TypeError):
            # En caso de error en parseo de la fila, se omite el registro corrupto
            continue

    return lote_transformado


# =============================================================================
# FASE 3: LOAD - CARGA EN LOTE (BATCH LOAD) EN BASE DE DATOS
# =============================================================================
def cargar_lote_sqlite(conn, lote_transformado):
    """
    Carga un lote de registros transformados en la base de datos SQLite.
    """
    sql = """
    INSERT INTO admisiones_emergencia 
    (id_admision, fecha_ingreso, id_paciente, cama_asignada, diagnostico, 
     costo_consulta, comision_seguro, costo_neto, alerta_gravedad, estado_paciente)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    cursor = conn.cursor()
    cursor.executemany(sql, lote_transformado)
    conn.commit()


def cargar_lote_postgres(conn, lote_transformado):
    """
    Carga un lote de registros transformados en PostgreSQL utilizando execute_values o executemany.
    """
    # TODO: SI USAN POSTGRESQL, IMPLEMENTAR AQUÍ CON PSYCOPG2
    sql = """
    INSERT INTO admisiones_emergencia 
    (id_admision, fecha_ingreso, id_paciente, cama_asignada, diagnostico, 
     costo_consulta, comision_seguro, costo_neto, alerta_gravedad, estado_paciente)
    VALUES %s;
    """
    cursor = conn.cursor()
    execute_values(cursor, sql, lote_transformado)
    conn.commit()


# =============================================================================
# EJECUCIÓN PRINCIPAL Y MONITOREO DE MEMORIA RAM
# =============================================================================
def ejecutar_pipeline():
    directorio_base = os.path.dirname(os.path.abspath(__file__))
    ruta_csv = os.path.join(directorio_base, "logs_admisiones_masivas.csv")
    ruta_db_sqlite = os.path.join(directorio_base, "taller_etl_resultado.db")

    print("=" * 70)
    print(" INICIANDO PIPELINE ETL CON GENERADORES - CLÍNICA SAN JOSÉ ")
    print("=" * 70)

    # Medición de consumo de memoria RAM (tracemalloc)
    tracemalloc.start()
    tiempo_inicio = time.time()

    # Inicializar Base de Datos Destino (Ejemplo local en SQLite)
    conn = sqlite3.connect(ruta_db_sqlite)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS admisiones_emergencia (
            id_admision TEXT PRIMARY KEY,
            fecha_ingreso TEXT,
            id_paciente TEXT,
            cama_asignada TEXT,
            diagnostico TEXT,
            costo_consulta REAL,
            comision_seguro REAL,
            costo_neto REAL,
            alerta_gravedad INTEGER,
            estado_paciente TEXT
        );
    """)
    conn.commit()

    total_procesados = 0
    total_lotes = 0

    print("-> Procesando archivo masivo en lotes (Streaming via yield)...")

    # Bucle del ETL: Iteramos directamente sobre el GENERADOR
    for lote_raw in extractor_lotes_csv(ruta_csv, tamano_lote=5000):
        total_lotes += 1
        
        # 1. Transformar lote
        lote_listo = transformar_lote(lote_raw)

        # 2. Cargar lote
        cargar_lote_sqlite(conn, lote_listo)

        total_procesados += len(lote_listo)

        # Reportar estado
        peak_ram_mb = tracemalloc.get_traced_memory()[1] / (1024 * 1024)
        print(f"   [Lote #{total_lotes:02d}] Cargadas {len(lote_listo):,} filas | Acumulado: {total_procesados:,} | RAM Pico: {peak_ram_mb:.2f} MB")

    conn.close()
    duracion = time.time() - tiempo_inicio
    memoria_final_mb = tracemalloc.get_traced_memory()[1] / (1024 * 1024)
    tracemalloc.stop()

    print("\n-------------------------------------------------------------------")
    print(" SUMMARY DE RENDIMIENTO DEL PIPELINE:")
    print("-------------------------------------------------------------------")
    print(f" - Filas totales cargadas con éxito: {total_procesados:,}")
    print(f" - Lotes procesados:                 {total_lotes}")
    print(f" - Tiempo de ejecución:              {duracion:.2f} segundos")
    print(f" - Consumo máximo de RAM (Pico RAM): {memoria_final_mb:.2f} MB")
    print("===================================================================")
    print(" [ÉXITO] El consumo de RAM se mantuvo CONSTANTE gracias al uso de yield.")

if __name__ == "__main__":
    ejecutar_pipeline()
