
rm(list = ls())

library(readr)
library(dplyr)
library(tidyverse)

# define the path and file name
path_unzip <- "unzip files/"
destfile <- "survey_1_answers_20250502T075229.zip"

# # download zip
# curl::curl_download(url, destfile = paste(path_zip, destfile, sep = "/"))

unzip(destfile, exdir = path_unzip)

# get the list of questions files 
files <- unzip(destfile, list = TRUE)
questions <- grep('questionid', files$Name, ignore.case = TRUE, value=TRUE)

# list of uesless columns (drop them)
unwanted_cols <- c('ID', 'QuestionID', 'SectionID', 'SubmittedAt', 'Duration')

# questions that are multiple choice - convert the answers into one list
multi_answer <- c("Q03", "Q05", "Q06", 
                  "QA1", "QA2", "QA3", "QA4", "QA5", "QA6", "QA7", "QA8", "QA9", "QA11", 
                  "QB1", "QB2", "QB3", "QB4a", "QB4b", 
                  "QF3")

# variable that want to convert into seperate columns
stats_cols <- c("QA1", "QA3","QA5", "QA6", "QA7", "QA8", "QA9", "QA11", 
                "QB1", "QB2", "QB3")

convert_structure <- function(data, question_id) {
  col_names <- names(data %>% select(starts_with(question_id)))
  statement_col <- paste0(question_id, "_", "Statement")
  row_col <- paste0(question_id, "_", "Row")
  
  if (statement_col %in% col_names){
    transformed_data <- data %>%
      unnest(cols = all_of(col_names)) %>%
      group_by(SurveySessionID) %>%
      # mutate(statement_number = {{statement_col}}) %>%
      arrange(SurveySessionID, !!sym(statement_col)) %>%
      ungroup() %>%
      pivot_wider(
        id_cols = SurveySessionID,
        names_from = !!sym(statement_col),
        values_from = setdiff(col_names, statement_col),
        names_glue = "{.name}"
      ) 
  } else if (row_col %in% col_names) {
    transformed_data <- data %>%
      unnest(cols = all_of(col_names)) %>%
      group_by(SurveySessionID) %>%
      # mutate(statement_number = {{statement_col}}) %>%
      arrange(SurveySessionID, !!sym(row_col)) %>%
      ungroup() %>%
      pivot_wider(
        id_cols = SurveySessionID,
        names_from = !!sym(row_col),
        values_from = setdiff(col_names, row_col),
        names_glue = "{.name}"
      ) 
  }
  return(transformed_data)
}

process_file <- function(file_path) {
  df <- read.csv(paste0(path_unzip, file_path), stringsAsFactors = FALSE)
  
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
  
  # group by ID for multiple choice questions
  if (question_id %in% multi_answer) {
    df_clean  <- df_clean %>%
      group_by(SurveySessionID) %>%
      summarise(across(everything(), ~ list(.x)), .groups = "drop")
  }
  
  if (question_id %in% stats_cols) {
    df_clean <- convert_structure(df_clean, question_id)
  }

  return(df_clean)
}

data_list <- lapply(questions, process_file)

# merge all data frames using a full join
merged_data <- Reduce(function(x, y) merge(x, y, by = "SurveySessionID", all = T), data_list)

# convert list values into plain text to save as .csv (separate by ;)
merged_data <- merged_data %>%
  mutate(across(where(is.list), ~ sapply(.x, function(x) paste(x, collapse = ";"))))

# save the final dataset
write.csv(merged_data, "integrated_data.csv")

