#!/bin/bash

# word_counter.sh
# Takes one argument: the input file path
# Outputs the word count to stdout

# Basic error handling
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <input_file>" >&2
    exit 1
fi

input_file="$1"

if [ ! -f "$input_file" ]; then
    echo "Error: Input file not found: $input_file" >&2
    exit 1
fi

# Execute word count and output ONLY the count number
# Use awk to ensure only the number is printed, handling potential extra output from wc
wc -w < "$input_file" | awk '{print $1}'

exit 0