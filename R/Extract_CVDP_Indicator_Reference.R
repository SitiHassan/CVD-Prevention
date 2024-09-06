#1. INDICATOR REFERENCE TABLE ----------------------------------------------------

##1. Create a list of all possible combinations of 'timePeriodID', 'systemLevelID'                                                                #
##2. Write a function that fetches the JSON data for a given combination.         
##3. Extract the necessary data for each combination and append it to a list.     
##4. Convert the combined list into a single data frame and remove duplicates.     


##1.1. Create a list of all possible combinations ---------------------------------

timePeriods <- 9
systemLevels <- c(1, 2, 3, 4, 5, 6, 7, 8)

# Create a tibble containing all possible combinations of indicators, time periods, & system levels
combinations <- tibble(timePeriodID = timePeriods, systemLevelID = systemLevels) %>% 
  expand(timePeriodID, systemLevelID)


##1.2. Function to fetch JSON data ------------------------------------------------

fetch_data <- function(timePeriodID, systemLevelID){
  
  # Construct URL
  url <- sprintf("https://api.cvdprevent.nhs.uk/indicator/list?timePeriodID=%s&systemLevelID=%s",
                 timePeriodID,  systemLevelID)
  
  # Fetch data
  response <- GET(url)
  
  # Check if the request was successful
  if(http_status(response)$category == "Success"){
    
    # Save the content to a file
    json_data <- content(response, "parsed")
    
    return(json_data) 
    
  }else{
    
    return(NULL) # Return NULL if the request is not successful
  }
  
}

##1.3. Fetching and combining the data --------------------------------------------

## Apply function to each row of a tibble (e.g., a combination of indicatorID, timePeriod, systemLevel)
## Result is a list where each element corresponds to the JSON response for each combination
## Apply function to each element of a list and row-binds the results together into a single tibble
## The function being applied is inside '{}'
## For each JSON response, access IndicatorList components including IndicatorCode, IndicatorName, IndicatorShortName, and IndicatorID
## Bring it all together and save all individual tibbles into one large tibble

all_indicators <- combinations %>% 
  purrr::pmap(fetch_data) %>% 
  purrr::map_dfr(~{
    if(!is.null(.x$indicatorList) && length(.x$indicatorList) > 0
       && is.list(.x$indicatorList[[1]])){
      map_dfr(.x$indicatorList, ~tibble(
        IndicatorCode = .x$IndicatorCode,
        IndicatorName = .x$IndicatorName,
        IndicatorShortName = .x$IndicatorShortName,
        IndicatorID = .x$IndicatorID))
      
    }else{
      NULL
    }
    
    
  })

##1.4. Removing duplicates---------------------------------------------------------

unique_indicators <- all_indicators %>% 
  distinct(IndicatorCode, IndicatorName, IndicatorShortName, IndicatorID, .keep_all = TRUE)

file_name <- "BSOL_1255_CVDPREVENT_IndicatorReference.xlsx"

write_xlsx(unique_indicators, path = file.path(directory, file_name))
