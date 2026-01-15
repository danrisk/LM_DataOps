library(tidyverse)
library(openxlsx)
library(readr)
library(readxl)

options(scipen = 999)
options(digits=7)

#### conexiones versión database
contabilidad <- DBI::dbConnect(odbc::odbc(),
                               Driver   = "ODBC Driver 17 for SQL Server",
                               Server   = "192.168.8.14",
                               Database = "CMUNDIAL",
                               UID      = "danny2",
                               PWD      = "ReadyLove100*",
                               Port     = 1433)


con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "ODBC Driver 17 for SQL Server",
                      Server   = "172.30.149.67",
                      Database = "Sis2000",
                      UID      = "dmorales",
                      PWD      = "lamundial*2025*morales",
                      Port     = 1433)


rms <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "ODBC Driver 17 for SQL Server",
                      Server   = "172.30.149.67",
                      Database = "Sis2000",
                      UID      = "dmorales",
                      PWD      = "lamundial*2025*morales",
                      Port     = 1433)





# Cargar librerías necesarias
library(pointblank)
library(dplyr)
library(DBI)

# 1. Simulación de conexión a tu base de datos On-premise
# con = dbConnect(RSQLite::SQLite(), ":memory:") 

# 2. Definir el "Agente de Validación"
agent <- create_agent(
  read_fn = ~ tbl(con, "Siniestros"), # Aquí conectas a tu tabla real
  tbl_name = "Validación_Siniestros_2026",
  label = "Control de Calidad DataOps"
) %>%
  # Regla 1: El ID de Siniestro no puede ser nulo
  col_is_not_null(columns = vars(ID_Siniestro)) %>%
  # Regla 2: El monto de reserva debe ser positivo
  col_vals_gt(columns = vars(Monto_Reserva), value = 0) %>%
  # Regla 3: Las fechas deben ser coherentes (no mayores a hoy)
  col_vals_lte(columns = vars(Fecha_Ocurrencia), value = Sys.Date()) %>%
  # Regla 4: El estado del siniestro debe estar dentro del catálogo oficial
  col_vals_in_set(columns = vars(Estado), set = c("Abierto", "Cerrado", "En Proceso")) %>%
  interrogate()

# 3. Generar el reporte visual
print(agent)

# 4. Exportar resultados para el equipo de TI (DataOps Feedback Loop)
# x_write_disk(agent, filename = "reporte_calidad_siniestros.html")