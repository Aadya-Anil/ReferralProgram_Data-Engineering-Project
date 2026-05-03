# Referral Program Data Pipeline

## Overview
This project processes referral program data to identify valid and potentially fraudulent referral rewards using Python and Pandas, containerized with Docker.

## Project Structure
📂 project  
├── 📁 data  
├── 📁 profiled datasets  
├── 📁 output  
│   ├── 📄 referral_report.csv     
├── 📄 Dockerfile    
├── 📄 data_dictionary.xlsx  
├── 📄 main.py  
├── 📄 requirements.txt  
├── 📄 vercel.json  
├── 📄 index.html  

Each component serves a specific purpose:

* data               : Stores raw input datasets
* profiled datasets  : Stores profiled datasets with null count and distinct value count
* output             : Contains generated output report
* Dockerfile         : Docker configuration
* data_dictionary    : Data dictionary for business users
* main.py            : Main pipeline script
* requirements.txt   : Python dependencies
* vercel.json        : Configuration file to deploy index.html and bypass main.py
* index.html         : Interactive web dashboard that visualizes the referral analytics report.
  
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
  
- user_referrals.csv
- user_referral_logs.csv
- user_logs.csv
- user_referral_statuses.csv
- referral_rewards.csv
- paid_transactions.csv
- lead_log.csv

## Output
The pipeline generates `output/referral_report.csv` with 46 rows containing referral details and a boolean `is_business_logic_valid` column indicating whether each referral reward is valid or potentially fraudulent.
To view the visualisation of the project, click the link below. 

Live Vercel link : https://referral-program-data-engineering.vercel.app

<img width="2520" height="1530" alt="Springer Capital — Referral Analytics_1" src="https://github.com/user-attachments/assets/8029bb03-db38-4704-9789-342f15605c32" />

