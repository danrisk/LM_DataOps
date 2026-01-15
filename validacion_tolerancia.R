# En la UI (Interfaz)
tabPanel("Historial de Auditoría",
         DTOutput("tabla_logs")
)

# En el Server
output$tabla_logs <- renderDT({
  # Conectamos a la tabla de logs que llena Airflow
  query <- "SELECT * FROM LOG_CONCILIACIONES ORDER BY fecha_ejecucion DESC"
  logs <- dbGetQuery(con, query)
  
  datatable(logs) %>%
    formatStyle(
      'estado',
      backgroundColor = styleEqual(c('FALLO', 'OK'), c('#ffcccc', '#ccffcc'))
    )
})