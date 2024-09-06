library(tidyverse)
library(jsonlite)
library(httr)
library(odbc)
library(DBI)
library(purrr)
library(writexl)

rm(list = ls())

start_time = Sys.time()

#1. Function to get the maximum time period ------------------------------------

get_MaxTimePeriod <- function() {
  
  base_url <- "https://api.cvdprevent.nhs.uk/timePeriod"
  
  response <- GET(base_url)
  
  if (http_status(response)$category == "Success") {
    timePeriod_data <- content(response, "parsed")
    
    timePeriods <- timePeriod_data$timePeriodList %>%
      map_int("TimePeriodID")
    
    max_timePeriod <- max(timePeriods)
    
    return(max_timePeriod)
    
  } else {
    return(NULL)
  }
}



#2. Function to get the available indicator IDs --------------------------------

timePeriod <- get_MaxTimePeriod()
systemLevels <- c( 1,2,3,4,5,6,7,8)
combinations <- expand_grid(timePeriodID = timePeriod, systemLevelID = systemLevels)

# Function to fetch JSON data for indicators and extract Indicator IDs
get_and_extract_indicators <- function(timePeriodID, systemLevelID) {
  url <- sprintf("https://api.cvdprevent.nhs.uk/indicator/list?timePeriodID=%s&systemLevelID=%s",
                 timePeriodID, systemLevelID)
  
  response <- GET(url)
  
  if (http_status(response)$category == "Success") {
    
    data <- content(response, "parsed")
    
    if (!is.null(data$indicatorList) && length(data$indicatorList) > 0) {
      return(tibble(
        IndicatorID = map_int(data$indicatorList, "IndicatorID")
      ))
      
    # If the 'indicatorList' contains NULL elements / length is 0
    } else {
      return(tibble()) # Returns an empty dataframe
    }
    
    # If the response is not "Success"
  } else {
    return(tibble()) # Returns an empty dataframe
  }
}

# Fetch and combine the data
fetch_all_indicators <- function(combinations) {
  all_indicators <- combinations %>%
    purrr::pmap(get_and_extract_indicators) %>% # Applies function to each row of the 'combinations' tibble
    purrr::compact() %>% # Removes NULL elements
    bind_rows() # Combines the results into a single dataframe
  
  return(all_indicators)
}

all_indicators <- fetch_all_indicators(combinations) %>% distinct() %>% pull()

#3. Extract all CVD Prevent data -----------------------------------------------
# Function to extract data for all combinations

Extract_CVDP <- function(IndicatorID, AreaTypeID, TimePeriodID) {
  
  # Initialize dataframes
  raw_data <- data.frame()
  invalid_combinations <- data.frame("timeperiod" = integer(),
                                     "system_level"= integer(),
                                     "indicatorID" = integer(),
                                     "error_message" = character(),
                                     stringsAsFactors = FALSE)
  
  cat("This process may take several minutes...\n")
  
  # Create a list of all combinations
  combinations <- expand_grid(TimePeriodID = TimePeriodID, AreaTypeID = AreaTypeID, IndicatorID = IndicatorID)
  
  # Use purrr::pmap to process combinations in parallel
  results <- combinations %>%
    pmap(function(TimePeriodID, AreaTypeID, IndicatorID) {
      
      cat(sprintf("Extracting data for TimePeriodID: %s, SystemLevelID: %s, IndicatorID: %s\n",
                  TimePeriodID, AreaTypeID, IndicatorID))
      
      result <- tryCatch({
        # Build the URL
        csv_url <- sprintf("https://api.cvdprevent.nhs.uk/indicator/%s/rawDataCSV?timePeriodID=%s&systemLevelID=%s",
                           IndicatorID, TimePeriodID, AreaTypeID)
        
        # Fetch the data
        temp_data <- read.csv(csv_url) %>%
          mutate(AreaType = case_when(
            AreaTypeID == 1 ~ "CTRY", 
            AreaTypeID == 2 ~ "STP",
            AreaTypeID == 3 ~ "CCG", 
            AreaTypeID == 4 ~ "PCN",
            AreaTypeID == 5 ~ "Practice", 
            AreaTypeID == 6 ~ "RGN",
            AreaTypeID == 7 ~ "ICB", 
            AreaTypeID == 8 ~ "LOC"), 
            .after = "AreaName")
        
        list(data = temp_data, error = NULL)
        
      }, error = function(e) {
        list(data = NULL, error = paste(conditionMessage(e)))
      })
      
      if (!is.null(result$data)) {
        raw_data <<- bind_rows(raw_data, result$data)
        
      } else {
        invalid_combinations <<- bind_rows(invalid_combinations, tibble(
          timeperiod = TimePeriodID,
          system_level = AreaTypeID,
          indicatorID = IndicatorID,
          error_message = result$error
        ))
      }
      
      return(NULL)  # To make sure pmap doesn't return a list of results
    })
  
  cat("Extraction completed\n")
  
  return(list("data" = raw_data, "invalid_combinations" = invalid_combinations))
}

# Extract data
result <- Extract_CVDP(all_indicators, systemLevels, timePeriod)

# Function to get maximum length of character columns
get_max_char_lengths <- function(df) {
  df %>%
    summarise(across(where(is.character), ~max(nchar(.), na.rm = TRUE)))
}

# Apply the function to result$data
max_char_lengths <- get_max_char_lengths(result$data)
print(max_char_lengths)


# Write into database ----------------------------------------------------------

sql_connection <-
  dbConnect(
    odbc(),
    Driver = "SQL Server",
    Server = "MLCSU-BI-SQL",
    Database = "Working",
    Trusted_Connection = "True"
  )

# Overwrite the existing table
dbWriteTable(
  sql_connection,
  Id(schema = "dbo", table = "BSOL_1255_CVD_Staging_Table"),
  result$data,
  append = TRUE,
  row.names = FALSE
)

end_time <- Sys.time()
time_taken <- end_time - start_time

print(paste0("Time taken to run this script: ", time_taken))