directory <- "./Reports/01_Adhoc/BSOL_1255_CVD_Prevent_Data" 



#3. CVD PREVENT METADATA EXTRACTION --------------------------------------------

##3.1 Function to extract metadata for each indicator ---------------------------

get_indicator_metadata <- function(IndicatorID){
  # Define the URL
  base_url <- "https://api.cvdprevent.nhs.uk/indicator"
  full_url <- paste0(base_url, "/", IndicatorID, "/details")
  
  cat("Constructed URL:", full_url, "\n")
  
  # Fetch the data from the URL
  response = GET(full_url)
  
  cat("HTTP Status:", http_status(response)$message, "\n")
  
  # Check if the request was successful
  if(http_type(response) != "application/json"){
    stop(paste("Failed to retrieve data for indicatiorID:", IndicatorID, ".Please try again later."))
  }
  
  # Parse the JSON response
  data_list <- fromJSON(content(response, "text", encoding = "UTF-8") )
  
  cat("Indicator Code in Response:", data_list$indicatorDetails$IndicatorCode, "\n")
  
  return(data_list$indicatorDetails)
  
}

##3.2 Load function -------------------------------------------------------------
all_metadata <- vector("list", 34)

for (id in 1:34){
  
  cat("Fetching data for indicator ID:", id, "\n")
  
  # Add data to the list by index
  all_metadata[[paste("Indicator", id)]] <- get_indicator_metadata(id)
  
  Sys.sleep(1)
}

##3.3. Function to process the metadata for a single indicator ------------------
process_indicator_metadata <- function(metadata, indicator_name, indicator_id, indicator_code){
  
  # Process each section
  processed_sections <-lapply(names(metadata), function(section_name){
    
    #Extract the section data frame
    section_df <- metadata[[section_name]]
    
    # Add a new column with the section name
    section_df$SectionName <- section_name
    
    return(section_df)
    
  })
  
  # Combine all processed sections vertically and add indicator_columns
  bind_rows (processed_sections) %>% 
    mutate(IndicatorName = indicator_name,
           IndicatorID = indicator_id,
           IndicatorCode = indicator_code)
  
}

##3.4. Load the function to extract metadata for all indicators -----------------

all_processed_metadata <- lapply(1:length(all_metadata), function(ind){
  indicator_name <- names(all_metadata)[ind]
  indicator_id <- paste("ID_", ind)
  indicator_code <- all_metadata[[indicator_name]]$IndicatorCode
  process_indicator_metadata(all_metadata[[indicator_name]]$MetaData,
                             indicator_name, indicator_id, indicator_code)
})

final_metadata_df <- bind_rows(all_processed_metadata) %>% 
  select(c(1, 2, 9), c(3,  5, 6, 7, 8))

file_name <- "BSOL_1255_CVDPREVENT_Metadata.xlsx"

write_xlsx(final_metadata_df, path = file.path(directory, file_name))

