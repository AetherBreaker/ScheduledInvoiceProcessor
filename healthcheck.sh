#!/usr/bin/env

FILE_PATH="timestamp_file.txt"
TIME_LIMIT_MINUTES=3

# 1. Read the ISO datetime from the first line of the file
#    The 'tr' command is used to replace 'T' and potential time zone indicators with a space for better compatibility with 'date -d'
#    'cut' is used to remove any potential extra information after the time, like milliseconds
file_datetime_str=$(head -n 1 "$FILE_PATH" | cut -d'.' -f1 | tr 'T' ' ')

# Check if file_datetime_str is empty
if [[ -z "$file_datetime_str" ]]; then
    echo "Error: Could not read date from file or file is empty."
    exit 1
fi

# 2. Convert the file datetime string to a Unix timestamp (seconds since epoch)
#    We suppress errors in case the date format is invalid using &>/dev/null
file_timestamp=$(date -d "$file_datetime_str" +%s 2>/dev/null)

if [[ $? -ne 0 ]]; then
    echo "Error: Invalid date format in file: $file_datetime_str"
    exit 1
fi

# 3. Get the current time minus 3 minutes as a Unix timestamp
now_minus_3min_timestamp=$(date -d "now - $TIME_LIMIT_MINUTES minutes" +%s)

# 4. Compare the timestamps
if [[ "$file_timestamp" -gt "$now_minus_3min_timestamp" ]]; then
    echo "The file date ($file_datetime_str) IS GREATER than now minus $TIME_LIMIT_MINUTES minutes."
    exit 0
else
    echo "The file date ($file_datetime_str) is NOT greater than now minus $TIME_LIMIT_MINUTES minutes."
    exit 1
fi
