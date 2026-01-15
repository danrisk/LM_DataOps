library(dplyr)
library(readr)

# 1. Extracción (Ejemplo con CSV y DB)
data_a <- read_csv("https://api.empresa.com/v1/reporte.csv")
data_b <- read_csv("datos_locales_db.csv")

# 2. Lógica de Reconciliación
reporte_final <- full_join(data_a, data_b, by = "id_transaccion", suffix = c("_src1", "_src2")) %>%
  mutate(
    diferencia_monto = monto_src1 - monto_src2,
    status = case_when(
      is.na(monto_src1) ~ "Faltante en Fuente A",
      is.na(monto_src2) ~ "Faltante en Fuente B",
      diferencia_monto != 0 ~ "Discrepancia de Valor",
      TRUE ~ "OK"
    )
  ) %>%
  filter(status != "OK")

# 3. Guardar el resultado en un lugar accesible para Shiny
write_csv(reporte_final, "/shared_folder/resultado_reconciliacion.csv")





library(shiny)
library(DT)
library(dplyr)

ui <- fluidPage(
  titlePanel("Módulo de Descarga de Reportes de Reconciliación"),
  
  sidebarLayout(
    sidebarPanel(
      helpText("Este reporte muestra las diferencias encontradas por el motor Airflow."),
      downloadButton("download_csv", "Descargar Reporte Completo (CSV)")
    ),
    
    mainPanel(
      h3("Resumen de Discrepancias"),
      DTOutput("tabla_diferencias")
    )
  )
)

server <- function(input, output) {
  
  # Leer el archivo generado por Airflow
  datos_procesados <- reactive({
    read.csv("/shared_folder/resultado_reconciliacion.csv")
  })
  
  # Mostrar tabla interactiva
  output$tabla_diferencias <- renderDT({
    datatable(datos_procesados(), options = list(pageLength = 10))
  })
  
  # Manejador de descarga
  output$download_csv <- downloadHandler(
    filename = function() {
      paste("reporte-diferencias-", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(datos_procesados(), file, row.names = FALSE)
    }
  )
}

shinyApp(ui = ui, server = server)






### dinamica


# 1. Renderizar la tabla principal (Resumen)
output$tabla_resumen <- renderDT({
  resumen <- dbGetQuery(con, "SELECT id_ejecucion, fecha_proceso, periodo_contable, estado_validacion FROM Auditoria_Conciliaciones")
  datatable(resumen, selection = 'single') # Permite seleccionar una fila
})

# 2. Reaccionar a la selección del usuario
observeEvent(input$tabla_resumen_rows_selected, {
  fila <- input$tabla_resumen_rows_selected
  id_ejec <- resumen_data()$id_ejecucion[fila]
  
  # 3. Consultar el detalle solo de esa ejecución
  output$tabla_detalle <- renderDT({
    query_detalle <- sprintf("SELECT id_registro_externo, valor_a, valor_b, tipo_error 
                              FROM Auditoria_Discrepancias_Items 
                              WHERE id_ejecucion = %d", id_ejec)
    detalle <- dbGetQuery(con, query_detalle)
    datatable(detalle, caption = "Detalle de Discrepancias Encontradas")
  })
})