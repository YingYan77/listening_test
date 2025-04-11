

library(readr)
library(dplyr)



# url <- "url-to-your-zip"
# path_zip <- "your-downloaded-zip-local-path"
# path_unzip <- "unzip files/"
destfile <- "survey_1_answers_20250409T144323.zip"

# # download zip
# curl::curl_download(url, destfile = paste(path_zip, destfile, sep = "/"))

# get the list of questions files 
files <- unzip(destfile, list = TRUE)
questions <- grep('questionid', files$Name, ignore.case = TRUE, value=TRUE)

# select uesless columns (drop them)
unwanted_cols <- c('ID', 'QuestionID', 'SectionID', 'SubmittedAt', 'Duration')

process_file <- function(file_path) {
  df <- read.csv(file_path, stringsAsFactors = FALSE)
  
  # Extract the question ID (assuming there's a column named "QuestionID")
  question_id <- unique(df$QuestionName)
  
  if (length(question_id) != 1) {
    stop(paste("More than one QuestionID in file:", file_path))
  }
  
  # Drop unwanted columns
  df_clean <- df[ , !(names(df) %in% unwanted_cols)]
  
  # Drop QuestionID after using it (or keep it if needed)
  df_clean <- df_clean %>% select(-QuestionName)
  
  # Identify the ID column (assumed to be named "id")
  id_col <- "SurveySessionID"
  
  # Rename all columns (except ID) to include the question ID
  colnames(df_clean) <- sapply(colnames(df_clean), function(col) {
    if (col != id_col) paste0(question_id, "_", col) else col
  })
  
  return(df_clean)
}

data_list <- lapply(questions, process_file)

# Merge all data frames using a full join by "id"
merged_data <- Reduce(function(x, y) merge(x, y, by = "SurveySessionID"), data_list)

write.csv(merged_data, "integrated_data.csv")