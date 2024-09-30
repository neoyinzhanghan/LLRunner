library(parallel)
library(hhsmm)
library(readr)
library(progress)

# Define data paths
data_paths <- c("/home/greg/Documents/neo/Zhiyuan/RHP1.csv", 
                "/home/greg/Documents/neo/Zhiyuan/RHP2.csv", 
                "/home/greg/Documents/neo/Zhiyuan/RHP3.csv", 
                "/home/greg/Documents/neo/Zhiyuan/RHP4.csv", 
                "/home/greg/Documents/neo/Zhiyuan/RHP5.csv", 
                "/home/greg/Documents/neo/Zhiyuan/RHP6.csv", 
                "/home/greg/Documents/neo/Zhiyuan/RHP7.csv")

compositions <- c(1,2,3,4,5,6,7)
n_repeats <- 5
sojourns <- c("weibull", "logarithmic", "gamma", "poisson")

get_model_path <- function(save_dir, composition, sojourn, i) {
  path <- file.path(save_dir, paste("RHP", composition, "_", sojourn, "_", i, ".rda", sep=""))
  return (path)
}

string_to_matrix <- function(string) {
  # Strip whitespace from the string
  digit_string <- trimws(string)
  
  # Convert string to a vector of digits
  digit_vector <- as.numeric(unlist(strsplit(digit_string, "")))
  
  # Check for any NA in digit_vector (safety check)
  if (any(is.na(digit_vector))) {
    stop(paste("NA values found in string:", string))
  }
  
  # Return the vector of digits
  return(digit_vector)
}

# Define the prepare_data_all_tao function
prepare_data_all_tao <- function(data) {
  # Subset the data to only include rows 1:4500
  data <- data[4501:5000, ]
  
  # Print the first few sequences for debugging
  print("First few sequences before processing:")
  print(head(data$sequence))
  
  # Convert the "sequence" column to individual digits, ensuring only valid digits (0-9) are kept
  data$sequence <- lapply(data$sequence, function(x) {
    # Use the string_to_matrix function to convert the sequence
    digits_matrix <- string_to_matrix(x)
    
    # Filter only valid digits (0 to 9)
    valid_digits <- digits_matrix[digits_matrix %in% c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)]
    
    # Debugging: print the valid digits for the first few sequences
    if (length(valid_digits) == 0) {
      print(paste("Warning: no valid digits found in sequence:", x))
    }
    
    return(valid_digits)
  })
  
  # Check for any NA values after conversion (this is a safety check)
  if (any(is.na(unlist(data$sequence)))) {
    stop("There are NA values in the sequence data after conversion.")
  }
  
  # Concatenate all sequences into a single list of digits
  concatenated_digits <- unlist(data$sequence)
  
  # Print the total number of concatenated digits for debugging
  print(paste("Total number of digits in concatenated sequences:", length(concatenated_digits)))
  
  # Create the X_matrix of shape [N, 1] where N is the total number of digits
  N <- length(concatenated_digits)
  X_matrix <- matrix(concatenated_digits, nrow = N, ncol = 1, byrow = TRUE)
  
  # Print the dimensions of X_matrix for debugging
  print(paste("Dimensions of X_matrix:", dim(X_matrix)))
  
  # Use the provided seq_len column from the CSV file to create the seq_lens matrix
  seq_lengths <- data$seq_len
  k <- length(seq_lengths)
  seq_lens <- matrix(seq_lengths, nrow = k, ncol = 1, byrow = TRUE)
  
  # Print the sum of seq_lens and compare it with the number of rows in X_matrix
  print(paste("Sum of seq_lens:", sum(seq_lens)))
  print(paste("Number of rows in X_matrix:", nrow(X_matrix)))
  
  # Check if the sum of sequence lengths matches the number of rows in X_matrix
  if (sum(seq_lens) != nrow(X_matrix)) {
    stop(paste("Mismatch detected. Sum of sequence lengths:", sum(seq_lens), 
               "Number of rows in X_matrix:", nrow(X_matrix)))
  }
  
  prepared_data <- hhsmmdata(X_matrix, seq_lens)
  
  # Return both X_matrix and seq_lens
  return(prepared_data)
}


# Morteza's new score function
score_debugged <- function (xnew, fit, ...)
{
  if (mode(xnew) == "numeric" | mode(xnew) == "integer") {
    if (is.null(dim(xnew))) {
      N = nrow(xnew <- t(as.matrix(xnew)))
    }
    else {
      N = nrow(xnew <- as.matrix(xnew))
    }
  }
  else {
    N = xnew$N
    xnew = as.matrix(xnew$x)
  }
  Nc = c(0, cumsum(N))
  score = c()
  for (i in 1:length(N)) {
    xx = matrix(xnew[(Nc[i] + 1):(Nc[i + 1]), ], (Nc[i + 1])-(Nc[i] + 1)+1, ncol(xnew))
    suppressWarnings(score <- c(score, hhsmmfit(xx, fit$model,
                                                fit$mstep, ..., M = fit$M, par = list(maxit = 1, verbose = FALSE))$loglik))
  }
  return(score)
}

results_df <- data.frame(data_path = character(), 
                         model_path = character(),
                         composition = character(),
                         sojourn = character(),
                         iteration = integer(),
                         score = numeric(),
                         stringsAsFactors = FALSE)

# Define the total number of iterations for the progress bar
total_iterations <- length(data_paths) * n_repeats * length(sojourns)

# Initialize the progress bar
pb <- progress_bar$new(
  format = "  [:bar] :current/:total (:percent) Elapsed: :elapsed Remaining: :eta",
  total = total_iterations,
  clear = FALSE,
  width = 60
)
# Function to process each combination of data_path, repeat, and sojourn
process_combination <- function(data_path, i, sojourn) {
  cat("Processing data path:", data_path, "Repeat:", i, "Sojourn:", sojourn, "\n")
  pb$tick()
  
  composition <- sub(".*RHP(\\d+)\\.csv$", "\\1", data_path)
  
  model_path <- get_model_path(save_dir = "/home/greg/Documents/neo/Zhiyuan_models",
                               composition = composition,
                               sojourn = sojourn,
                               i = i)
  
  cat("Loading model from:", model_path, "\n")
  load(model_path)
  
  tao_data <- read.csv(data_path, colClasses = c("sequence" = "character"))
  prepared_tao_data <- prepare_data_all_tao(tao_data)
  
  cat("Scoring data...\n")
  score <- sum(score_debugged(xnew = prepared_tao_data, fit = fit, n = 4))
  
  results_row <- data.frame(data_path = data_path, 
                            model_path = model_path,
                            composition = composition,
                            sojourn = sojourn,
                            iteration = i,
                            score = score,
                            stringsAsFactors = FALSE)
  
  # Extracting the names from the paths
  model_name <- basename(model_path)
  data_name <- basename(data_path)
  
  # Removing extensions
  model_name <- sub("\\.rda$", "", model_name)
  data_name <- sub("\\.csv$", "", data_name)
  
  # Creating the output file name
  output_file_name <- paste0(model_name, "---", data_name, ".csv")
  
  # Define the output directory
  output_directory <- "/home/greg/Documents/neo/Zhiyuan_models/predict_test_1"
  
  # Full path for the output file
  output_file_path <- file.path(output_directory, output_file_name)
  
  cat("Predicting and saving to:", output_file_path, "\n")
  pred <- predict(object = fit, newdata = prepared_tao_data, future = 0, method = "viterbi", RUL.estimate = FALSE,
                  confidence = "max", conf.level = 0.95, n = 4)

  seq_len = prepared_tao_data$N

  print(pred)

  # exit the system
    stop("Exiting the system")
    
  
  # Create a data frame with the sequence as a single string
  df <- data.frame(sequence = sequence_str, stringsAsFactors = FALSE)
  
  write.csv(df, output_file_path, row.names = FALSE)
  
  cat("Completed processing for:", data_path, "\n")
  
  return(results_row)
}

# Parallel processing
all_results <- mclapply(1:n_repeats, function(repeat_num) {
  cat("Starting repeat:", repeat_num, "\n")
  mclapply(sojourns, function(sojourn) {
    cat("Processing sojourn:", sojourn, "\n")
    mclapply(data_paths, function(data_path) {
      process_combination(data_path, repeat_num, sojourn)
    }, mc.cores = 20)  # Parallelize over datasets
  }, mc.cores = length(sojourns))  # Parallelize over sojourns
}, mc.cores = n_repeats)  # Parallelize over repeats

# Combine the results
results_df <- do.call(rbind, do.call(rbind, all_results))

# Save the combined results
cat("Saving combined results...\n")
write.csv(results_df, "/home/greg/Documents/neo/Zhiyuan_models/predict_test_1/results.csv", row.names = FALSE)
cat("All processes completed successfully!\n")
