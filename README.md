# Coffee-Shop-Sales-Analysis-Data-Warehousing-using-MySQL
SQL-based analysis of coffee shop sales data including data cleaning, transformation, and performance insights using MySQL.
# ☕ Coffee Shop Sales Analysis (MySQL Project)

This project is based on analyzing coffee shop sales data using MySQL.
The idea was to work on something close to real-world data and understand how raw data can be cleaned and used for analysis.

---

## 📌 Project Overview

In this project, I have taken a sales dataset of a coffee shop and performed data cleaning, transformation and analysis using SQL.

Instead of directly working on clean data, I first created a staging table and then built proper tables from it. This helped in understanding how actual data pipelines work in real scenarios.

The project mainly focuses on finding useful insights like sales trends, top products, and customer behavior.

---

## 🛠️ Tools Used

* MySQL
* SQL (MySQL Workbench)

---

## 📂 Dataset Information

The dataset contains transaction-level data of a coffee shop.

Some of the main fields are:

* transaction id and timestamp
* product name and category
* price and quantity
* customer details
* store location and type

The data was not perfectly clean, so basic cleaning was done while loading and transforming it.

---

## 🧱 Data Structure

The project follows a simple data warehouse approach:

* **Staging Table** → raw data loaded as it is
* **Dimension Tables** → store, product, customer, date
* **Fact Table** → main sales data (transactions)

This structure made it easier to run analysis queries later.

---

## 📊 Key Analysis Done

Some of the analysis performed:

* Total revenue and total orders
* Top 5 selling products
* Monthly and daily sales trends
* Customer lifetime value
* Store performance ranking
* Peak sales hours
* Repeat vs new customers

Also tried some advanced queries like:

* ranking using window functions
* moving average
* running total and growth percentage

---

## 💡 What I Learned

* How to handle raw data using staging tables
* Basics of data warehousing (fact & dimension tables)
* Writing better SQL queries using joins and aggregations
* Using window functions for deeper analysis
* How real datasets are not always clean

---

## ▶️ How to Run This Project

1. Open MySQL Workbench
2. Run the SQL file step by step
3. Make sure dataset path is correct in `LOAD DATA INFILE`
4. Execute queries and check results

---

## ⚠️ Notes

* File paths may need to be changed based on your system
* Some data cleaning is done inside SQL itself
* This project is mainly for practice and learning purpose

---

## 📁 Project File

* `coffeeshop final project.sql` → contains full code (table creation + analysis queries)

---

## 🙋‍♂️ Author

Kartik Thakkar

---

## ⭐ Final Thoughts

This project helped me understand how SQL is used beyond basic queries.
Working with structured tables and analyzing data step by step gave a better idea of how real data analysis works.

Still learning and improving 🙂
