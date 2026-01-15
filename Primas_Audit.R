library(pointblank)
library(dplyr)
library(lubridate)

# 1. Preparación de datos (Simulando unión de tablas de tu DB On-prem)
# En producción usarías: tbl(con, "Recibos") %>% left_join(tbl(con, "Comisiones"), ...)
datos_auditoria <- data_frame_recaudos %>% 
  mutate(
    calculo_total_impuestos = prima_neta + derecho_emision + impuestos_iva,
    diferencia_prima = abs(prima_total - calculo_total_impuestos)
  )

# 2. Creación del Agente de Validación de Primas y Comisiones
agente_financiero <- create_agent(
  read_fn = ~ datos_auditoria,
  tbl_name = "Auditoria_Financiera_Primas",
  label = "DataOps: Control de Integridad Financiera"
) %>%
  
  # --- SECCIÓN A: INTEGRIDAD DE PRIMAS ---
  
  # A1. Validación del cálculo de Prima Total (Margen de error de 0.01 por redondeo)
  col_vals_lte(
    columns = vars(diferencia_prima),
    value = 0.01,
    label = "Cuadratura Prima Total vs Componentes"
  ) %>%
  
  # A2. Prima Neta debe ser positiva (Evitar ingresos negativos mal registrados)
  col_vals_gt(
    columns = vars(prima_neta),
    value = 0,
    label = "Prima Neta Positiva"
  ) %>%
  
  # --- SECCIÓN B: LÓGICA DE COMISIONES ---
  
  # B1. Comisión no puede exceder el monto de la prima neta
  col_vals_expr(
    expr = ~ monto_comision < prima_neta,
    label = "Comisión < Prima Neta"
  ) %>%
  
  # B2. Validar Tasa de Comisión (Ej: No debe superar el 30% según política interna)
  col_vals_expr(
    expr = ~ (monto_comision / prima_neta) <= 0.30,
    label = "Tasa Comisión dentro de Rango (Máx 30%)"
  ) %>%
  
  # B3. Si la póliza está anulada, la comisión debe ser cero o negativa (devolución)
  col_vals_expr(
    expr = ~ ifelse(estado_poliza == "ANULADA", monto_comision <= 0, TRUE),
    label = "Comisiones en Pólizas Anuladas"
  ) %>%
  
  # --- SECCIÓN C: INTEGRIDAD REFERENCIAL ---
  
  # C1. Todo recibo debe tener un productor (agente) asignado
  col_is_not_null(
    columns = vars(id_productor),
    label = "Existencia de Intermediario"
  ) %>%
  
  interrogate()

# 3. Visualizar Reporte
print(agente_financiero)
