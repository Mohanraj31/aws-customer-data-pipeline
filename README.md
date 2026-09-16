# Customer & Order Data Migration Project

## Overview

This project implements a data engineering migration pipeline using AWS Glue, Amazon S3, and MongoDB Atlas.

Customer and order data is initially stored as CSV files in Amazon S3. The data is validated, cleansed, joined, and transformed into a nested customer-oriented structure. The transformed data is stored as Parquet in S3 and then migrated to MongoDB Atlas.

## Architecture

S3 CSV Files
      |
      v
AWS Glue Job 1 - Transformation
      |
      |-- Data validation
      |-- Null checks
      |-- Duplicate checks
      |-- Data cleansing
      |-- Customer/Order LEFT JOIN
      |-- Nested orders array
      |
      v
S3 - Transformed Parquet
      |
      v
AWS Glue Job 2 - Migration
      |
      v
MongoDB Atlas
      |
      v
customer_order_migration.customers

## Source Data

### Customer

- customer_id
- first_name
- last_name
- email
- city
- state
- signup_date
- customer_segment

### Order

- order_id
- customer_id
- order_date
- order_status
- order_channel
- payment_method
- shipping_city

## Transformation

The customer and order datasets are joined using:

`customer.customer_id = order.customer_id`

A LEFT JOIN is used so that all customers are retained.

The orders are transformed into a nested array within each customer document.

Example:

```json
{
  "customer_id": "CUST0001",
  "first_name": "Divya",
  "last_name": "Kumar",
  "email": "divya.kumar@example.com",
  "city": "Pune",
  "state": "Maharashtra",
  "customer_segment": "Retail",
  "orders": [
    {
      "order_id": "ORD00001",
      "order_date": "2025-10-29",
      "order_status": "DELIVERED",
      "order_channel": "WEB",
      "payment_method": "DEBIT_CARD",
      "shipping_city": "Pune"
    }
  ]
}


Data Validation

The transformation process validates:

Null values
Duplicate customer IDs
Duplicate order IDs
Orphan orders
Data types
String whitespace

Final validation results:

Validation	Result
Customers	120
Orders	800
Customer nulls	0
Order nulls	0
Duplicate customer IDs	0
Duplicate order IDs	0
Orphan orders	0
Final customers	120
Customers without orders	0
Total nested orders	800
AWS Services
Amazon S3
AWS Glue
AWS IAM
Amazon VPC
NAT Gateway
Amazon CloudWatch
MongoDB

MongoDB Atlas is used as the target database.

Database:

customer_order_migration

Collection:

customers

The AWS Glue connection is:

mongodb_atlas_connection

Credentials are managed through the AWS Glue connection and are not stored in the source code.

Project Structure

migration-project/
│
├── transformation/
│   └── customer_order_transformation.py
│
├── migration/
│   ├── config.py
│   └── s3_to_mongodb.py
│
├── tests/
│   ├── test_migration.py
│   └── test_transformation.py
│
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   └── products.csv
│
├── .gitignore
├── requirements.txt
└── README.md


Issues Encountered and Solutions
1. Local Windows Parquet issue

While testing PySpark locally, Parquet writing required Hadoop native Windows support.

Solution: The transformation was executed in AWS Glue, where the transformed Parquet output was successfully written to S3.

2. Source schema issue

The order schema initially did not contain all required fields.

Solution: The schema was corrected to include shipping_city and all required order fields.

3. Date and whitespace handling

Date fields were initially strings and source values required trimming.

Solution: String columns were trimmed and date fields were converted using the yyyy-MM-dd format.

4. MongoDB Atlas hostname issue

An incorrect Atlas hostname caused DNS resolution to fail.

Solution: The Atlas hostname was corrected to:

customer-order-cluster.cfahbai.mongodb.net

5. AWS Glue VPC configuration

The Glue connection required VPC, subnet, and security-group configuration.

Solution: The Glue connection was configured with the appropriate VPC, subnet, security group, and NAT Gateway.

6. MongoDB TLS connection failure

The Glue job initially failed with:

SSLException: Received fatal alert: internal_error

Solution: The Glue connection and Atlas network configuration were investigated. The Atlas IP Access List was updated to allow the AWS Glue NAT Gateway public IP.

7. Atlas IP Access List issue

Atlas initially allowed the workstation public IP but not the AWS Glue NAT Gateway IP.

Solution: The NAT Gateway public IP was added to the Atlas IP Access List.

8. Invalid MongoDB connection option

One run reported an invalid connection string when retryWrites=false was included.

Solution: The unnecessary retryWrites option was removed from the Glue job connection options.

9. Target database did not exist

The Glue connection test reported that:

customer_order_migration

did not exist.

Solution: The database and customers collection were created in MongoDB Atlas.

Final Result

The migration was successfully completed.

MongoDB Atlas verification:

Database: customer_order_migration
Collection: customers
Customer documents: 120
Nested orders are present inside customer documents.
Security Notes

Do not commit:

AWS credentials
MongoDB passwords
Connection strings containing passwords
.env files
Private keys
Raw production/customer data

Credentials should be managed through AWS Glue connections or an appropriate secrets-management solution.


