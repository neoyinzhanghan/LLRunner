library(parallel)
library(hhsmm)
library(readr)
library(progress)


# Define data paths
target_protein_data_path <- "~/Documents/neo/Zhiyuan/Zhiyuan_target_prot.csv"

save_dir <- "~/Documents/neo/Zhiyuan_models"

compositions <- c(1,2,3,4,5,6,7)
n_repeats <- 5
sojourns <- c("weibull", "logarithmic", "gamma", "poisson")

# Helper function to get model path
get_model_path <- function(save_dir, composition, sojourn, i) {
  path <- file.path(save_dir, paste("RHP", composition, "_", sojourn, "_", i, ".rda", sep=""))
  return (path)
}

# Helper function to convert string to matrix
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

# Load target protein data
target_protein_data <- read.csv(target_protein_data_path, colClasses = c("sequence" = "character"))

# Define the total number of iterations for the progress bar, multiply by number of rows in target protein data
total_iterations <- n_repeats * length(sojourns) * length(compositions) * nrow(target_protein_data)

# Initialize the progress bar
pb <- progress_bar$new(
  format = "  [:bar] :current/:total (:percent) Elapsed: :elapsed Remaining: :eta",
  total = total_iterations,
  clear = FALSE,
  width = 60
)

# Function to load the string, convert to matrix, load model, and score
get_string_and_score <- function(save_dir, composition, sojourn, i, target_protein_name) {
  # Load the target protein data
  target_protein_data <- read.csv(target_protein_data_path, colClasses = c("sequence" = "character"))
  
  # Filter for the specific target protein
  target_protein_data <- target_protein_data[target_protein_data$group == target_protein_name, ]
  
  # Ensure there's only one row left
  if (nrow(target_protein_data) != 1) {
    stop("Expected exactly one row for the target protein data, but found:", nrow(target_protein_data))
  }
  
  # Get the sequence string and convert to matrix
  seq_len <- nchar(target_protein_data$sequence)
  X_matrix <- matrix(string_to_matrix(target_protein_data$sequence), nrow = seq_len, ncol = 1)
  seq_lens <- matrix(seq_len, nrow = 1, ncol = 1)
  
  # Prepare data
  prepared_data <- hhsmmdata(X_matrix, seq_lens)
  
  # Load the model
  model_path <- get_model_path(save_dir, composition, sojourn, i)
  load(model_path)
  
  score <- tryCatch({
    score_debugged(xnew = prepared_data, fit = fit, n = 4)
  }, error = function(e) {
    NA  # Assign NA in case of an error
  })

  # Predict the sequence
  pred <- predict(object = fit, newdata = prepared_data, future = 0, method = "viterbi", RUL.estimate = FALSE,
                  confidence = "max", conf.level = 0.95, n = 4)

  # the pred_string is the predicted sequence as a string
  pred_string <- paste(pred$s, collapse = "")
  
  # Return the predicted sequence and score
  return(list(pred_string, score))
}

# Create an empty dataframe to store the results
results_df <- data.frame(pred = character(),
                         score = numeric(),
                         composition = integer(),
                         sojourn = character(),
                         iteration = integer(),
                         group = character(),
                         stringsAsFactors = FALSE)

# Loop through each combination of composition, sojourn, repeat, and protein group
groups <- unique(target_protein_data$group)

for (composition in compositions) {
  for (sojourn in sojourns) {
    for (i in 1:n_repeats) {
      for (group in groups) {
        # Run the model for each protein group and get the result
        result <- get_string_and_score(save_dir, composition, sojourn, i, group)
        
        # Add result to the dataframe
        results_df <- rbind(results_df, data.frame(pred = result[[1]],
                                                   score = result[[2]],
                                                   composition = composition,
                                                   sojourn = sojourn,
                                                   iteration = i,
                                                   group = group,
                                                   stringsAsFactors = FALSE))
        
        # Update progress bar
        pb$tick()
      }
    }
  }
}

# After the loop, results_df will contain all the results
# save the results to a CSV file in the save_dir named target_protein_results.csv
write.csv(results_df, file.path(save_dir, "target_protein_results.csv"), row.names = FALSE)