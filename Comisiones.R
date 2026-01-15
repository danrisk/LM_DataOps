library(tidyverse)
library(openxlsx)
library(readr)
library(readxl)

options(scipen = 999)
options(digits=7)


reporte_comisiones <- read_excel("reporte_comisiones.xlsx")
reporte_recibos <- read_excel("recibo_0110.xlsx")
#reporte_contabilidad_comisiones <- 

primas_rec <- primas_rec |> 
  mutate(Saldo = Haber - Debe)
# Lógica de Conciliación
reporte_comparacion <- left_join(primas_rec,recibos_detalles_comparacion,
                                 by = c("Numero_recibo"= "Nro de Recibo")) |>
  # filter(`Tipo de Movimiento` == "Comision",
  #         `Fecha de pago de Comision` >= "01-10-2025",
  #         `Fecha de pago de Comision` <= "10-10-2025") |> 
  mutate(Diferencia_primas = `Prima Bruta` - Saldo,
         Estado = case_when(
           is.na(`Prima Bruta`) ~ "Falta en sistema",
           is.na(Saldo) ~ "Falta en contabilidad",
           Diferencia_primas != 0 ~ "Discrepancia de Valor",
           TRUE ~ "OK"
         )) |> 
  filter(Estado != "OK")


write.xlsx(reporte_comparacion, "comparativo_primas.xlsx")




library(dplyr)

# Cargar fuentes
data_a <- read.csv("fuente_a.csv")
data_b <- read.csv("fuente_b.csv")

# Comparación completa (Outer Join)
reporte <- full_join(data_a, data_b, by = "id_transaccion", suffix = c("_A", "_B")) %>%
  mutate(
    estado = case_when(
      is.na(monto_A) ~ "Solo en Fuente B",
      is.na(monto_B) ~ "Solo en Fuente A",
      monto_A != monto_B ~ "Diferencia de Monto",
      TRUE ~ "OK"
    )
  ) %>%
  filter(estado != "OK") # Solo nos interesan las discrepancias

write.csv(reporte, "reporte_discrepancias.csv")


            