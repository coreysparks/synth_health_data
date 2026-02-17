install.packages(c("DBI", "RPostgres", "dplyr"))

library(DBI)
library(RPostgres)
library(dbplyr)

con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = 5433,
  dbname = "postgres",
  user = "postgres",
  password = "postgres"
)

library(dplyr)

DBI::dbListObjects(con)

dbGetQuery(
  con,
  "
  select table_name
  from information_schema.tables
  where table_schema = 'synth'
  order by table_name
"
)


fact <- dbGetQuery(con, "select * from synth.fact_inpatient_stay limit 10")
fact

con |>
  tbl(in_schema("synth", "fact_inpatient_stay")) |>
  head(10) |>
  collect()

DBI::dbDisconnect(con)
