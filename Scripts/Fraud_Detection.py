from pyhive import hive
import csv
import os

def write_to_csv(results, file_path, header):
    file_exists = os.path.isfile(file_path)
    
    with open(file_path, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        
        if not file_exists:
            csvwriter.writerow(header)
        
        csvwriter.writerows(results)

def execute_query_and_save_to_csv(query, file_path, header):
    conn = hive.Connection(host='localhost', port=10000)
    database_name = 'financial_data'

    with conn.cursor() as cursor:
        cursor.execute(f"USE {database_name}")
        cursor.execute(query)
        results = cursor.fetchall()

    write_to_csv(results, file_path, header)
    print(f"Data has been appended to {file_path}")

# Query to identify unusually high transaction amounts
unusually_high_query = """
SELECT t.*
FROM transactions t
JOIN (
    SELECT percentile_approx(amountUSD, 0.95) AS percentile_95
    FROM transactions
) p ON t.amountUSD > p.percentile_95
"""
unusually_high_file_path = 'C:/Users/Youcode/Desktop/8 Months/sprint 4/fifth week_Détection de Fraude dans les Transactions Financières/Transactions_Detection_archive/Unusually_High_Transaction_Amounts.csv'
unusually_high_header = ['amount', 'currency', 'amountUSD', 'customer_id', 'transaction_date', 'time', 'location', 'merchant_details', 'transaction_id', 'transaction_type', 'year', 'month', 'day']

execute_query_and_save_to_csv(unusually_high_query, unusually_high_file_path, unusually_high_header)

# Query to identify High Frequency of Transactions Within a Short Time Frame
high_frequency_query = """
SELECT customer_id, COUNT(*) AS transaction_count
FROM transactions
WHERE transaction_date BETWEEN '2022-12-02' AND '2022-12-29'
GROUP BY customer_id
HAVING COUNT(*) > 1
"""
high_frequency_file_path = 'C:/Users/Youcode/Desktop/8 Months/sprint 4/fifth week_Détection de Fraude dans les Transactions Financières/Transactions_Detection_archive/High_Frequency_of_Transactions_within_a_Short_Time_Frame.csv'
high_frequency_header = ['customer_id', 'transaction_count']

execute_query_and_save_to_csv(high_frequency_query, high_frequency_file_path, high_frequency_header)

# Query to identify Transactions from Unusual Locations
unusual_locations_query = """
SELECT t.*
FROM transactions t
LEFT JOIN customers c ON t.location = c.location
WHERE c.customer_id IS NULL
"""
unusual_locations_file_path = 'C:/Users/Youcode/Desktop/8 Months/sprint 4/fifth week_Détection de Fraude dans les Transactions Financières/Transactions_Detection_archive/Transactions_from_Unusual_Locations.csv'
unusual_locations_header = ['amount', 'currency', 'amountUSD', 'customer_id', 'transaction_date', 'time', 'location', 'merchant_details', 'transaction_id', 'transaction_type', 'year', 'month', 'day']

execute_query_and_save_to_csv(unusual_locations_query, unusual_locations_file_path, unusual_locations_header)

# Query to identify Transactions Involving Blacklisted Customers
blacklisted_customers_query = """
SELECT t.*
FROM transactions t
JOIN blacklist_info b ON t.merchant_details = b.blacklist
"""
blacklisted_customers_file_path = 'C:/Users/Youcode/Desktop/8 Months/sprint 4/fifth week_Détection de Fraude dans les Transactions Financières/Transactions_Detection_archive/Transactions_Involving_Blacklisted_Customers.csv'
blacklisted_customers_header = ['amount', 'currency', 'amountUSD', 'customer_id', 'transaction_date', 'time', 'location', 'merchant_details', 'transaction_id', 'transaction_type', 'year', 'month', 'day']

execute_query_and_save_to_csv(blacklisted_customers_query, blacklisted_customers_file_path, blacklisted_customers_header)