# Referral Program Data Pipeline

## Overview
This project processes referral program data to identify valid and potentially fraudulent referral rewards using Python and Pandas, containerized with Docker.

## Project Structure
Project\ data

## Requirements
- Docker Desktop
- Python 3.10 (if running locally)

## Running with Docker

### 1. Build the container
```bash
docker build -t referral-pipeline .
```

### 2. Run the container and export report
```bash
docker run -v ${PWD}/output:/app/output referral-pipeline
```

### 3. Find your report
The output file will be saved to:
output/referral_report.csv

## Running Locally (without Docker)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the script
```bash
python main.py
```

## Input Data
Place all CSV files in the `data/` folder:
- user_referrals.csv
- user_referral_logs.csv
- user_logs.csv
- user_referral_statuses.csv
- referral_rewards.csv
- paid_transactions.csv
- lead_log.csv

## Output
The pipeline generates `output/referral_report.csv` with 46 rows containing 
referral details and a boolean `is_business_logic_valid` column indicating 
whether each referral reward is valid or potentially fraudulent.
