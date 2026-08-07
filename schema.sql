-- ============================================================
-- BASE DE DATOS MYSQL PARA EL TALLER ETL
-- CLÍNICA SAN JOSÉ
-- ============================================================

CREATE DATABASE IF NOT EXISTS clinica_san_jose
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE clinica_san_jose;

-- ATENCIÓN:
-- Este DROP elimina la tabla para comenzar una prueba limpia.
DROP TABLE IF EXISTS admisiones_emergencia;

CREATE TABLE admisiones_emergencia (
    id_admision VARCHAR(36) PRIMARY KEY,
    fecha_ingreso DATETIME NOT NULL,
    id_paciente VARCHAR(10) NOT NULL,
    cama_asignada VARCHAR(30) NOT NULL,
    diagnostico VARCHAR(100) NOT NULL,
    costo_consulta DECIMAL(10, 2) NOT NULL,
    comision_seguro DECIMAL(10, 2) NOT NULL,
    costo_neto DECIMAL(10, 2) NOT NULL,
    alerta_gravedad BOOLEAN DEFAULT FALSE,
    estado_paciente VARCHAR(20) NOT NULL,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_admisiones_fecha (fecha_ingreso),
    INDEX idx_admisiones_paciente (id_paciente),
    INDEX idx_admisiones_diagnostico (diagnostico)
) ENGINE=InnoDB;