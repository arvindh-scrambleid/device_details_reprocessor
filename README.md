# Device Name Reprocessor

A Node.js script that processes login logs from a CSV file and updates device records in DynamoDB.

## Requirements

- Node.js 12+
- AWS credentials configured in your environment

## Installation

```bash
npm install
```

## Usage

Set the required environment variables and run the script:

```bash
# Set environment variables
export ENV=dev  # The environment prefix for the DynamoDB table
export FILE_PATH=/path/to/your/logs.csv  # Path to the CSV file

# Run the script
npm start
```

## CSV Format

The CSV file should have the following columns:

- @timestamp
- sourceApp
- msg
- suid
- zid

## Cloudwatch Log Insight QL

This query is used to filter logs of successfull login from desktop agent (windows & mac)

```QL
fields @timestamp
| parse @message '"x-source-app":"*"' as sourceApp
| parse @message '"msg":"*"' as msg
| parse @message '"suid":"*"' as suid
| parse @message '"zid":"*"' as zid
| filter msg = "METRICS:record-login-response" and ((sourceApp) = 'windows' or (sourceApp) = 'mac')
```

## Process

1. The script scans the DynamoDB table to count items where:

   - recordType equals 'device'
   - type equals 'desktop'
   - name equals 'Desktop Agent'

2. For each record in the CSV file, it:

   - Queries the table with suid as partition key and "#device"+zid as sort key
   - Checks if the name attribute equals 'Desktop Agent' (case insensitive)
   - If matched, updates the name to include OS information: "Desktop Agent (OS)"
   - Adds a new 'os' attribute with the OS value

3. The script logs metrics about the process, including:
   - Total matching records in DynamoDB
   - Total records processed from CSV
   - Total records updated
