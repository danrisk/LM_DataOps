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



cobertura <- tbl(con, "ADPOLCOB") |> 
  filter(cramo == 18,
         fanopol == 2025,
         fmespol == 12,
         ccober == "1") |> 
  collect()

macoberturas <- tbl(con, "MACOBERTURAS") |>
  collect()


library(pointblank)
library(dplyr)
library(tidyr)

# 1. Preparación de datos (Simulando unión de tablas de Reaseguro)
# Necesitas: Polizas + Contratos_Reaseguro + Cesiones
datos_reaseguro <- tbl_cesiones_raw %>%
  mutate(
    total_cedido = monto_cesion_proporcional + monto_cesion_no_proporcional,
    porcentaje_cesion = (total_cedido / suma_asegurada_total) * 100,
    margen_retencion = suma_asegurada_total - total_cedido
  )

# 2. Agente de Validación para Operaciones de Reaseguro
agente_reaseguro <- create_agent(
  read_fn = ~ datos_reaseguro,
  tbl_name = "Calidad_Reaseguro_OnPrem",
  label = "DataOps: Auditoría de Cesión y Recuperos"
) %>%
  
  # --- SECCIÓN A: INTEGRIDAD DE LA CESIÓN ---
  
  # A1. Coherencia de Sumas: Lo cedido no puede ser mayor a la suma asegurada total
  col_vals_expr(
    expr = ~ total_cedido <= suma_asegurada_total,
    label = "Suma Cedida <= Suma Asegurada"
  ) %>%
  
  # A2. Validación de Plenos: Si la suma asegurada supera el límite (ej. 1M), debe haber reaseguro
  col_vals_expr(
    expr = ~ ifelse(suma_asegurada_total > 1000000, total_cedido > 0, TRUE),
    label = "Obligatoriedad de Cesión (>1M)"
  ) %>%
  
  # --- SECCIÓN B: RECOBRO DE SINIESTROS ---
  
  # B1. Consistencia de Recuperos: El recobro de reaseguro no puede superar el siniestro pagado
  col_vals_expr(
    expr = ~ monto_recupero_reaseguro <= monto_siniestro_pagado,
    label = "Recupero <= Siniestro Pagado"
  ) %>%
  
  # B2. Validación de Contrato: Todo siniestro cedido debe tener un ID de Contrato de Reaseguro válido
  col_is_not_null(
    columns = vars(id_contrato_reaseguro),
    label = "Existencia de Contrato Vinculado"
  ) %>%
  
  # --- SECCIÓN C: TEMPORALIDAD ---
  
  # C1. El contrato de reaseguro debe estar vigente en la fecha de ocurrencia del siniestro
  col_vals_expr(
    expr = ~ (fecha_ocurrencia_siniestro >= inicio_vigencia_reaseguro) & 
      (fecha_ocurrencia_siniestro <= fin_vigencia_reaseguro),
    label = "Siniestro dentro de Vigencia de Reaseguro"
  ) %>%
  
  interrogate()

# 3. Reporte de Resultados
print(agente_reaseguro)


# Dentro del server de Shiny
output$plot_exposicion <- renderPlot({
  
  # Simulamos el cálculo del Gap
  df_gap <- datos_reaseguro %>%
    filter(suma_asegurada_total > limite_retencion_cia) %>%
    mutate(estado_cesion = ifelse(total_cedido > 0, "Cedido", "NO CEDIDO (Riesgo)")) %>%
    group_by(ramo, estado_cesion) %>%
    summarise(monto_total = sum(suma_asegurada_total))
  
  ggplot(df_gap, aes(x = ramo, y = monto_total, fill = estado_cesion)) +
    geom_bar(stat = "identity", position = "stack") +
    scale_fill_manual(values = c("Cedido" = "#2ecc71", "NO CEDIDO (Riesgo)" = "#e74c3c")) +
    theme_minimal() +
    labs(title = "Exposición vs. Retención por Ramo",
         subtitle = "Alertas en rojo indican riesgos que exceden la capacidad propia sin reaseguro",
         y = "Monto Total Asegurado", x = "Ramo de Seguro")
})





# Si el monto en riesgo supera un umbral (ej. 1M), el script falla para que Airflow alerte
riesgo_critico <- datos_reaseguro %>%
  filter(suma_asegurada_total > 1000000 & total_cedido == 0) %>%
  summarise(total = sum(suma_asegurada_total))

if (riesgo_critico$total > 0) {
  stop(paste("CRITICAL: Se detectaron pólizas por", riesgo_critico$total, "sin respaldo de reaseguro."))
}



# Ejemplo de cómo registrar linaje en tu log
log_error_linaje <- data.frame(
  fecha = Sys.time(),
  tabla_destino = "Gold_Reaseguro",
  fuente_origen = "DB_Produccion.dbo.Cesiones_2026", # Linaje
  proceso_id = "ETL_REASEGURO_001",
  script_responsable = "auditoria_reaseguro.R",
  error_detectado = "Suma Asegurada > Límite"
)
