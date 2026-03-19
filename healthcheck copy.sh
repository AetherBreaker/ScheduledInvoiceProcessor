#!/usr/bin/env


# The ISO 8601 datetime string, including a timezone offset or 'Z' for UTC
ISO_STRING="2026-03-19T15:24:00.046808"

# Convert the string to a Unix timestamp
# -d specifies the input date string
# +%s specifies the output format as seconds since the Unix epoch
UNIX_TIMESTAMP=$(date -d "${ISO_STRING}" +"%s")

# Print the result
echo "ISO String: ${ISO_STRING}"
echo "Unix Timestamp: ${UNIX_TIMESTAMP}"
