#!/usr/bin/env bash

# Parameters
NUM_RECORDS=1000
NUM_CUSTOMERS=3
START_DATE="2025-07-01"
END_DATE="2025-07-02"

# Format the start date for the file name (YYYY-MM-DD -> YYYYMMDD)
START_DATE_FORMATTED=$(echo "$START_DATE" | tr -d '-')

# Generate leads CSV
python generate_model.py "leads_xs_${START_DATE_FORMATTED}.csv" $NUM_RECORDS $NUM_CUSTOMERS "$START_DATE" "$END_DATE"


