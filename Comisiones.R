library(tidyverse)
library(openxlsx)
library(readr)
library(readxl)
library(DBI)
library(odbc)


options(scipen = 999)
options(digits=7)



contabilidad <- DBI::dbConnect(odbc::odbc(),
                               Driver   = "ODBC Driver 17 for SQL Server",
                               Server   = "192.168.8.14",
                               Database = "CLAMUND",
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


maramos <- tbl(con, "MARAMOS") |> 
  collect()

RECIBOS <- tbl(con, "ADRECIBOS") |> 
  filter(fcobro >= "2025-01-01",
         fcobro <= "2025-12-31",
         iestadorec == "C") |> 
  group_by(cramo) |>
  summarise(monto_comision = sum(mcomision),
            Cantidad_recibos = n()) |>
  collect()


cuentas <- tbl(contabilidad, "SCCUENTA") |> 
  collect()

saldos <- tbl(contabilidad, "SCREN_CO") |> 
  filter(fec_emis >= as.Date("2025-01-01"),
         fec_emis <= as.Date("2025-12-31")) |> 
  collect()


Contabilidad <- left_join(saldos, cuentas, by = "co_cue")

Contabilidad_consolidada <- Contabilidad |> 
  mutate(saldo = abs(monto_d - monto_h),
         nro_recibo = str_extract(descri, "(?<=Nro_Recibo\\s|RECIBO\\s)[0-9-]+")) |> 
  select(co_cue, des_cue, nro_recibo, fec_emis, descri, monto_d, monto_h, saldo)




Comision_Resumen_SISIP <- RECIBOS |> 
  left_join(maramos) %>% 
  select(xdescripcion_l, monto_comision, Cantidad_recibos ) %>% 
  rename(Ramo = xdescripcion_l)

Comision_Resumen_Contabilidad <- Contabilidad_consolidada %>% 
  mutate(ramo = str_extract(des_cue, "(?<=COMISIONES -\\s|COMISION -\\s).*")) %>% 
  drop_na(ramo)

Comision_Resumen_CONTABILIDAD <- Comision_Resumen_Contabilidad %>% 
  group_by(ramo) %>% 
  summarise(monto_comision = sum(saldo))

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


write.xlsx(Comision_Resumen_SISIP, "Comisiones Consolidado SISIP.xlsx", overwrite = TRUE)




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


            