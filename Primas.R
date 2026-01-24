library(tidyverse)
library(openxlsx)
library(readr)
library(readxl)
library(DBI)
library(odbc)

options(scipen = 999)
options(digits=7)

#### conexiones versión database
PROFIT <- DBI::dbConnect(odbc::odbc(),
                               Driver   = "ODBC Driver 17 for SQL Server",
                               Server   = "192.168.8.14",
                               Database = "CMUNDIAL",
                               UID      = "danny2",
                               PWD      = "ReadyLove100*",
                               Port     = 1433)


SYSIP <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "ODBC Driver 17 for SQL Server",
                      Server   = "172.30.149.67",
                      Database = "Sis2000",
                      UID      = "dmorales",
                      PWD      = "lamundial*2025*morales",
                      Port     = 1433)


#######
Recibos <- tbl(SYSIP, "ADRECIBOS") |> 
  filter(
    fcobro >= "2026-01-01",
         fcobro <= "2026-01-15",
         iestadorec == "C") |> 
  collect()

maramos <- tbl(SYSIP, "MARAMOS") |> 
  collect()

Recibos_ramos <- Recibos |> 
  left_join(maramos, by ="cramo")
  
Recibos_detalle <- Recibos_ramos |> 
  select(cnpoliza, xdescripcion_l, femision, fdesde_pol, fhasta_pol, ctenedor, 
         cnrecibo, fdesde, fhasta, fcobro, cmoneda, ptasamon_pago, msumabruta, msumabrutaext, mprimabruta, mprimabrutaext,
         pcomision, mcomision, mcomisionext, mpcedida, mpcedidaext, mpret, mpretext, mpfp, mpfpext) |> 
  rename("Nº de Póliza" = cnpoliza,
         Ramo = xdescripcion_l,
         "Fecha de Emision Recibo" = femision,
         "Fecha desde Póliza" = fdesde_pol,
         "Fecha Hasta Póliza" = fhasta_pol,
         "Cédula Tomador" = ctenedor,
         "Nro de Recibo" = cnrecibo,
         "Fecha desde Recibo" = fdesde,
         "Fecha hasta Recibo" = fhasta,
         "Fecha de Cobro" = fcobro,
         Moneda = cmoneda,
         "Tasa de Cambio" = ptasamon_pago,
         "Suma Asegurada" = msumabruta,
         "Suma Asegurada Moneda Extranjera" = msumabrutaext,
         "Prima Bruta" = mprimabruta,
         "Prima Bruta Moneda Extranjera" = mprimabrutaext,
         "Porcentaje de Comisión" = pcomision,
         "Monto de Comisión" = mcomision,
         "Monto Comision Extranjera" = mcomisionext,
         "Prima Cedida en Reaseguro" = mpcedida,
         "Prima Cedida Moneda Extranjera"= mpcedidaext,
         "Prima Cedida Facultativo" = mpfp,
         "Prima Cedida Facultativo Moneda Extranjera" = mpfpext,
         "Prima Retenida" = mpret,
         "Prima Retenida Moneda Extranjera" = mpretext)


Poliza <- tbl(SYSIP, "ADPOLIZA") |> 
  filter(
    fanopol == 2025,
    fmespol %in% c(1:12),
    cramo == 18) |> 
  collect()


Poliza_auto <- Poliza %>% 
mutate(ramo_auto = str_detect(cplan, regex("rcv", ignore_case = TRUE)))

write.xlsx(prima_por_ramo, "prima_sisip.xlsx", overwrite = TRUE)
write.xlsx(Primas_Contabilidad_Diciembre, "cuadre_con.xlsx", overwrite = TRUE)

# Conciliación Auditada

Recibos_detalles_comparacion <- Recibos_ramos |> 
  select(cnpoliza, xdescripcion_l, femision, fdesde_pol, fhasta_pol, ctenedor, 
         crecibo, fdesde, fhasta, fcobro, cmoneda, ptasamon_pago, ptasamon, msumabruta, msumabrutaext, mprimabruta, mprimabrutaext,
         pcomision, mcomision, mcomisionext, mpcedida, mpcedidaext, mpret, mpretext, mpfp, mpfpext) |> 
  rename("Nº de Póliza" = cnpoliza,
         Ramo = xdescripcion_l,
         "Fecha de Emmision Recibo" = femision,
         "Fecha desde Póliza" = fdesde_pol,
         "Fecha Hasta Póliza" = fhasta_pol,
         "Cédula Tomador" = ctenedor,
         "Nro de Recibo" = crecibo,
         "Fecha desde Recibo" = fdesde,
         "Fecha hasta Recibo" = fhasta,
         "Fecha de Cobro" = fcobro,
         Moneda = cmoneda,
         "Tasa de Cambio al pago" = ptasamon_pago,
         "Tasa de Cambio" = ptasamon,
         "Suma Asegurada" = msumabruta,
         "Suma Asegurada Moneda Extranjera" = msumabrutaext,
         "Prima Bruta" = mprimabruta,
         "Prima Bruta Moneda Extranjera" = mprimabrutaext,
         "Porcentaje de Comisión" = pcomision,
         "Monto de Comisión" = mcomision,
         "Monto Comision Extranjera" = mcomisionext,
         "Prima Cedida en Reaseguro" = mpcedida,
         "Prima Cedida Moneda Extranjera"= mpcedidaext,
         "Prima Cedida Facultativo" = mpfp,
         "Prima Cedida Facultativ Moneda Extranjera" = mpfpext,
         "Prima Retenida" = mpret,
         "Prima Retenida Moneda Extranjera" = mpretext)

Primas_Contabilidad_Diciembre <- PRIMAS_DICIEMBRE |> 
  mutate(Saldo = Haber - Debe,
         Numero_recibo = str_extract(Descripción, "(?<=Nro_Recibo\\s)[0-9-]+")) |>
  arrange(Numero_recibo) |> 
  select(Numero_recibo, Cuenta, `Nombre Cuenta`, Fecha, Descripción, Saldo)



primas_consolidadas <- Recibos_detalles_comparacion |> 
  left_join(Primas_Contabilidad_Diciembre, by = c("Nro de Recibo" = "Numero_recibo"))
  # filter(`Tipo de Movimiento` == "Comision",
  #         `Fecha de pago de Comision` >= "01-10-2025",
  #         `Fecha de pago de Comision` <= "10-10-2025") |> 
  mutate(Diferencia_primas = `Prima Bruta` - Saldo,
         Estado = case_when(
           is.na(`Prima Bruta`) ~ "Falta en Data del Sistema",
           is.na(Saldo) ~ "Falta en Data contable",
           Diferencia_primas != 0 ~ "Discrepancia de Valor",
           TRUE ~ "OK"
         )) |> 
  filter(Estado != "OK")





# Base comparativa para Primas


# reporte_comparacion_Primas <- left_join(recibos_detalles_comparacion, ,
#                                  by = c("Nro_Recibo"="Nro. Recibo")) |>
#   filter(`Tipo de Movimiento` == "Comision",
#          `Fecha de pago de Comision` >= "01-10-2025",
#          `Fecha de pago de Comision` <= "10-10-2025") |> 
#   mutate(Diferencia_Comisiones = Comisión_BS - `Monto del Movimiento`,
#          Estado = case_when(
#            is.na(Comisión_BS) ~ "Falta en Recibos",
#            is.na(`Monto del Movimiento`) ~ "Falta en Comisiones",
#            Diferencia_Comisiones != 0 ~ "Discrepancia de Valor",
#            TRUE ~ "OK")) |> 
#   filter(Estado != "OK")


###############################################################################
  # Versión Archivos Locales
  
   primas_dic_2025 <- read_excel("primas_dic_2025.xlsx", sheet = "PRIMAS 01 AL 31 DIC")
  
  Primas_Tecnicas <- read_excel("~/Downloads/Primas_Tecnicas.xlsx")
  
   ramos <- read_excel("~/Downloads/ramos.xlsx")
   
   # version base de datos
   
   cuentas <- tbl(PROFIT, "SCCUENTA") |> 
     collect()
   
   saldos <- tbl(PROFIT, "SCREN_CO") |> 
     filter(fec_emis >= as.Date("2026-01-01"),
            fec_emis <= as.Date("2026-01-15")) |> 
     collect()
   
   
   Contabilidad <- left_join(saldos, cuentas, by = "co_cue")
   
   Contabilidad_consolidada <- Contabilidad |> 
     mutate(saldo = abs(monto_d - monto_h),
            nro_recibo = str_extract(descri, "(?<=Nro_Recibo\\s|RECIBO\\s)[0-9-]+")) |> 
     select(co_cue, des_cue, nro_recibo, fec_emis, descri, monto_d, monto_h, saldo) |>
     drop_na(nro_recibo)
  
  ###### Procesamos la información dessde su origen
  
  Prima_Contable <- primas_dic_2025 |> 
    mutate(Saldo = Haber - Debe,
           Nro_Recibo = str_extract(Descripción, "(?<=Nro_Recibo\\s)[0-9-]+")) |> 
    select(Cuenta, `Nombre Cuenta`, Nro_Recibo, Fecha, Debe, Haber, Saldo)
  
  
  prima_ramo <- Primas_Tecnicas |> 
    left_join(ramos, by = "cramo")
  
  
  Primas_Tecnicas <- prima_ramo |> 
    select(cnpoliza, xdescripcion_l, femision, fdesde_pol, fhasta_pol, ctenedor, 
           cnrecibo, fdesde, fhasta, fcobro, cmoneda, ptasamon_pago, ptasamon, msumabruta, msumabrutaext, mprimabruta, mprimabrutaext,
           pcomision, mcomision, mcomisionext, mpcedida, mpcedidaext, mpret, mpretext, mpfp, mpfpext) |> 
    rename("Nº de Póliza" = cnpoliza,
           Ramo = xdescripcion_l,
           "Fecha de Emision Recibo" = femision,
           "Fecha desde Póliza" = fdesde_pol,
           "Fecha Hasta Póliza" = fhasta_pol,
           "Cédula Tomador" = ctenedor,
           Nro_Recibo = cnrecibo,
           "Fecha desde Recibo" = fdesde,
           "Fecha hasta Recibo" = fhasta,
           "Fecha de Cobro" = fcobro,
           Moneda = cmoneda,
           "Tasa de Cambio al pago" = ptasamon_pago,
           "Tasa de Cambio" = ptasamon,
           "Suma Asegurada" = msumabruta,
           "Suma Asegurada Moneda Extranjera" = msumabrutaext,
           "Prima Bruta" = mprimabruta,
           "Prima Bruta Moneda Extranjera" = mprimabrutaext,
           "Porcentaje de Comisión" = pcomision,
           "Monto de Comisión" = mcomision,
           "Monto Comision Extranjera" = mcomisionext,
           "Prima Cedida en Reaseguro" = mpcedida,
           "Prima Cedida Moneda Extranjera"= mpcedidaext,
           "Prima Cedida Facultativo" = mpfp,
           "Prima Cedida Facultativ Moneda Extranjera" = mpfpext,
           "Prima Retenida" = mpret,
           "Prima Retenida Moneda Extranjera" = mpretext)

  Primas_auditadas <- Recibos_detalles_comparacion %>%  
    full_join( Contabilidad_consolidada, by = c("Nro de Recibo" = "nro_recibo")) 
  
  
  
  Primas_auditadas <-  Primas_auditadas |>
    drop_na(nro_recibo) |>
    group_by(nro_recibo) |>
    summarise(Monto_PROFIT = sum(saldo),
              Monto_SISIP = sum())
    
  
  contabilidad_ramo <- Contabilidad_consolidada %>% 
    group_by()
  
  prima_por_ramo <- Recibos_detalles_comparacion %>% 
    group_by(Ramo) %>% 
    summarise(prima = sum(`Prima Bruta`))
  
    sum(Contabilidad_consolidada$saldo)
    
    sum(Recibos_detalles_comparacion$`Prima Bruta`)
    
    mutate(Saldo = replace_na(Saldo,0),
           `Prima Bruta` = replace_na(`Prima Bruta`,0),
           `Porcentaje de Comisión` = replace_na(`Porcentaje de Comisión`,0),
           `Monto de Comisión` = replace_na(`Monto de Comisión`,0),
           `Prima Bruta Moneda Extranjera` = replace_na(`Prima Bruta Moneda Extranjera`,0),
           `Prima Cedida en Reaseguro` = replace_na(`Prima Cedida en Reaseguro`,0),
           `Prima Retenida` = replace_na(`Prima Retenida`,0),
           `Prima Retenida Moneda Extranjera` = replace_na(`Prima Retenida Moneda Extranjera`,0),
           `Prima Cedida Facultativo` = replace_na(`Prima Cedida Facultativo`,0),
           `Prima Cedida Facultativ Moneda Extranjera` = replace_na(`Prima Cedida Facultativ Moneda Extranjera`,0))
    
  
  
  
  sum(prima_por_ramo$prima)
  
  
 
  
  
  library(shiny)
  library(visNetwork)
  library(dplyr)
  
  ui <- fluidPage(
    titlePanel("Linaje de Datos: Flujo de Pólizas y Siniestros"),
    sidebarLayout(
      sidebarPanel(
        helpText("Haz clic en los nodos para ver detalles del proceso."),
        hr(),
        wellPanel(
          h4("Leyenda:"),
          tags$span(style="color:#AED6F1", "●"), " Origen (Legacy)", br(),
          tags$span(style="color:#F9E79F", "●"), " Transformación (Airflow)", br(),
          tags$span(style="color:#ABEBC6", "●"), " Destino (Consumo)"
        )
      ),
      mainPanel(
        visNetworkOutput("network", height = "600px")
      )
    )
  )
  
  server <- function(input, output) {
    output$network <- renderVisNetwork({
      
      # 1. Definición de Nodos (Representan sistemas, tablas o scripts)
      nodes <- data.frame(
        id = 1:7,
        label = c("AS/400 (Legacy)", "SQL Server Prod", 
                  "ETL Extracción", "Staging Area", 
                  "Script Calidad R", "Data Mart Actuarial", 
                  "Shiny Dashboard"),
        group = c("Origen", "Origen", "Proceso", "Proceso", "Proceso", "Destino", "Destino"),
        title = c("Base de datos DB2 - Seguros de Vida", "Producción General Autos", 
                  "Airflow Job: Ingesta_Daily", "Tabla Temporal On-Prem", 
                  "Validador pointblank.R", "Tabla final para Actuarios", 
                  "App de Visualización"),
        color = c("#AED6F1", "#AED6F1", "#F9E79F", "#F9E79F", "#F9E79F", "#ABEBC6", "#ABEBC6")
      )
      
      # 2. Definición de Conexiones (Edges)
      edges <- data.frame(
        from = c(1, 2, 3, 4, 5, 6),
        to   = c(3, 3, 4, 5, 6, 7)
      )
      
      # 3. Renderizar el Grafo
      visNetwork(nodes, edges) %>%
        visNodes(shape = "dot", shadow = TRUE) %>%
        visEdges(arrows = "to", color = list(color = "gray", highlight = "black")) %>%
        visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
        visLayout(randomSeed = 123) %>%
        visPhysics(solver = "forceAtlas2Based")
    })
  }
  
  shinyApp(ui = ui, server = server)
  
  
  
  
