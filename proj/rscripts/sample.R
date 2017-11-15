# sample.R
myArgs <- commandArgs(trailingOnly = TRUE)

# convert to numerics
nums = as.numeric(myArgs)

# print result
cat(max(nums))
