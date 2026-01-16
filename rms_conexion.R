library(tidyverse)
library(openxlsx)
library(readr)
library(readxl)
library(RPostgres)
library(DBI)
library(odbc)

options(scipen = 999)
options(digits=7)

#### conexiones versión database
RMS <- dbConnect(RPostgres::Postgres(),
                        dbname = 'rms40prd',
                        host = '172.30.149.68',
                        port = 5432,
                        user = 'danmorestad',
                        password = 'estad2025')
