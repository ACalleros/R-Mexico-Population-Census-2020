# La explicación del siguiente código en: https://abxda.medium.com/r-espacial-y-el-tidyverso-con-datos-censales-2020-f28e5314f157
#Instalación de paquetes

install.packages("tidyverse")

install.packages("R.utils")

if (!requireNamespace("BiocManager", quietly = TRUE))
  install.packages("BiocManager")

BiocManager::install("recount")

install.packages("sf")

# Inicializar funciones

library(tidyverse)
library(glue)
library(sf)
library(R.utils)
library(recount)

#Función de apoyo
extract_shapefile <- function(z,e,s){
  m<-c(paste(s,sep="","m.shp"),paste(s,sep="","m.dbf"),paste(s,sep="","m.prj"),paste(s,sep="","m.shx"))
  cd<-c(paste(s,sep="","cd.shp"),paste(s,sep="","cd.dbf"),paste(s,sep="","cd.prj"),paste(s,sep="","cd.shx"))
  pem<-c(paste(s,sep="","pem.shp"),paste(s,sep="","pem.dbf"),paste(s,sep="","pem.prj"),paste(s,sep="","pem.shx"))
  unzip(zipfile=z, files=c(m,cd,pem), exdir=e)
}

# Crear Directorios

mkdirs("inegi/ccpvagebmza/csv/conjunto_de_datos") 
mkdirs("inegi/mgccpv/shp/m/conjunto_de_datos") 
mkdirs("inegi/mgccpv/gpkg/") 

# Descarga Datos
directory <- "./inegi/ccpvagebmza/"
estado <- str_pad(1:32, 2, pad = "0")
df_estado <- tibble(estado, archivo = glue("{directory}ageb_mza_urbana_{estado}_cpv2020_csv.zip"), url=glue("https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/ageb_manzana/ageb_mza_urbana_{estado}_cpv2020_csv.zip"))
Map(function(u, d) download_retry(u, d, mode="wb",N.TRIES = 20L), df_estado %>% pull(url), df_estado %>% pull(archivo))

directory <- "./inegi/mgccpv/"
url_mgccpv <- "https://www.inegi.org.mx/contenidos/productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/marcogeo/889463807469/"
states <- c("01_aguascalientes.zip","02_bajacalifornia.zip","03_bajacaliforniasur.zip","04_campeche.zip","05_coahuiladezaragoza.zip","06_colima.zip","07_chiapas.zip","08_chihuahua.zip","09_ciudaddemexico.zip","10_durango.zip","11_guanajuato.zip","12_guerrero.zip","13_hidalgo.zip","14_jalisco.zip","15_mexico.zip","16_michoacandeocampo.zip","17_morelos.zip","18_nayarit.zip","19_nuevoleon.zip","20_oaxaca.zip","21_puebla.zip","22_queretaro.zip","23_quintanaroo.zip","24_sanluispotosi.zip","25_sinaloa.zip","26_sonora.zip","27_tabasco.zip","28_tamaulipas.zip","29_tlaxcala.zip","30_veracruzignaciodelallave.zip","31_yucatan.zip","32_zacatecas.zip")
df_states <- tibble(state=states,file=glue("{directory}{state}"), url=glue("{url_mgccpv}{state}"))
Map(function(u, d) download_retry(u, d, mode="wb",N.TRIES = 20L), df_states %>% pull(url), df_states %>% pull(file))

# Extracción de Datos
directory <- "inegi/ccpvagebmza/"
csv_dir <- paste(directory, sep = "","csv")
estado <- str_pad(1:32, 2, pad = "0")
df_estado <- tibble(estado, archivo = glue("{directory}ageb_mza_urbana_{estado}_cpv2020_csv.zip"), csv_dir=csv_dir, csv_file=glue("conjunto_de_datos/conjunto_de_datos_ageb_urbana_{estado}_cpv2020.csv"))
Map(function(z,f,e) unzip(zipfile=z, files=f, exdir=e), df_estado %>% pull(archivo), df_estado %>% pull(csv_file), df_estado %>% pull(csv_dir) )

directory <- "inegi/mgccpv/"
shp_dir <- paste(directory, sep = "","shp/m")
states <- c("01_aguascalientes.","02_bajacalifornia.","03_bajacaliforniasur.","04_campeche.","05_coahuiladezaragoza.","06_colima.","07_chiapas.","08_chihuahua.","09_ciudaddemexico.","10_durango.","11_guanajuato.","12_guerrero.","13_hidalgo.","14_jalisco.","15_mexico.","16_michoacandeocampo.","17_morelos.","18_nayarit.","19_nuevoleon.","20_oaxaca.","21_puebla.","22_queretaro.","23_quintanaroo.","24_sanluispotosi.","25_sinaloa.","26_sonora.","27_tabasco.","28_tamaulipas.","29_tlaxcala.","30_veracruzignaciodelallave.","31_yucatan.","32_zacatecas.")
df_states <- tibble(estado=estado, state=states,file=glue("{directory}{state}zip"), shp_dir=shp_dir,shp_file=glue("conjunto_de_datos/{estado}"))
Map(function(z,e,s) extract_shapefile(z,e,s), df_states %>% pull(file), df_states %>% pull(shp_dir), df_states %>% pull(shp_file) )

# Union de Datos
create_gpkg <- function(estado){
    print(paste("procesando estado: ", estado))
    df <- read_csv(glue("./inegi/ccpvagebmza/csv/conjunto_de_datos/conjunto_de_datos_ageb_urbana_{estado}_cpv2020.csv"), na = c('N/A','N/D','*'),)
    df_geo_censo <- df %>% filter(MZA != "000")
    df_geo_censo <- df_geo_censo %>% unite("CVEGEO", c("ENTIDAD", "MUN", "LOC", "AGEB", "MZA"),sep="",remove = FALSE)
    gpdf <- st_read(glue("./inegi/mgccpv/shp/m/conjunto_de_datos/{estado}m.shp"), quiet = T, options = "ENCODING=ISO-8859-1")
    cddf <- st_read(glue("./inegi/mgccpv/shp/m/conjunto_de_datos/{estado}cd.shp"), quiet = T, options = "ENCODING=ISO-8859-1")
    pemdf <- st_read(glue("./inegi/mgccpv/shp/m/conjunto_de_datos/{estado}pem.shp"), quiet = T, options = "ENCODING=ISO-8859-1")
    df_geo_censo <- df_geo_censo %>% left_join(gpdf, by = c('CVEGEO' = 'CVEGEO'))  %>% select (-c(CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA))
    #glimpse(df_geo_censo)
    df_full <- df_geo_censo %>% filter(!is.na(TIPOMZA))
    df1 <- df_geo_censo %>% filter(is.na(TIPOMZA)) %>% select (-c(TIPOMZA, AMBITO, geometry)) 
    df_geo_dif <- df1 %>% 
      left_join(pemdf, by = c('CVEGEO' = 'CVEGEO'))  %>% 
      select (-c(CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB)) %>% 
      left_join(cddf  %>% select(CVEGEO,TIPOMZA,AMBITO) %>% st_drop_geometry, by = c('CVEGEO' = 'CVEGEO')) 
    df_geo_cd <- df_geo_dif %>% select (-c(geometry))  %>% filter(is.na(CVE_MZA))  %>% select (-c(CVE_MZA)) 
    df_geo_cd <- df_geo_cd %>% left_join(cddf  %>% select(CVEGEO), by = c('CVEGEO' = 'CVEGEO')) 
    df_geo_dif <- df_geo_dif  %>% filter(!is.na(CVE_MZA))  %>% select (-c(CVE_MZA))
    final_shape <- union_all(df_full,union_all(df_geo_dif,df_geo_cd))
    print(paste("guardando datos del estado: ",estado))
    final_shape %>% st_write(glue("./inegi/mgccpv/gpkg/cpv2020_{estado}.gpkg"), "cpv2020")
}
start_time <- Sys.time()
estado <- str_pad(1:32, 2, pad = "0")
Map(function(e) create_gpkg(e), estado)
end_time <- Sys.time()
end_time - start_time
##
