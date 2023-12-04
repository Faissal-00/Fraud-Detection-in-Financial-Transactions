# Project Overview:
The project aims to develop a real-time fraud detection system to minimize false alerts. It involves several stages including API development, data collection, processing, storage using Hive, fraud detection rule-based system development, deployment using Airflow, and CI/CD integration using GitHub Actions.
The project consists of several code scripts that accomplish different tasks:

## 1. Data Generation:
Generates synthetic data for transactions, customers, and external data using Python.
Provides random transaction data including amounts, currencies, merchants, and customer IDs.
Generates high-frequency transactions for specific customers.

## 2. API Development:
-API of Transaction Data: Provides access to transactional data including transaction ID, date/time, amount, currency, merchant details, customer ID, and transaction type.
-API of Customer Data: Offers access to customer data like customer ID, account history, demographic information, and behavioral patterns.
-API of External Data: Retrieves external data like blacklist information, credit scores, and fraud reports.

## 3. ETL (Extract, Transform, Load):

Extracts data from APIs using Python requests.
Transforms transactional data including currency conversion to USD, parsing date/time, and organizing customer data.
Loads data into Hive tables for transactions, customers, and external data.

## 4. Fraud Detection:
Executes HiveQL queries to identify potential fraud activities based on certain criteria.
Identifies unusually high transaction amounts, high-frequency transactions within a short time frame, transactions from unusual locations, and transactions involving blacklisted customers.
Writes query results to CSV files for further analysis and reporting.

# Conclusion:
The project accomplishes data generation, API development, ETL processes, data storage, fraud detection rule implementation, and reporting using Hive and CSV files. The workflow is designed to detect suspicious transactions and minimize false alerts efficiently.
