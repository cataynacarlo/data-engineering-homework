#!/usr/bin/env python
# coding: utf-8

import sqlite3
import pandas as pd

conn = sqlite3.connect('./xyz_sales.db')

cursor = conn.cursor()

#get each table, assign it into a dataframe

sales_table_query = "SELECT * FROM sales"
orders_table_query = "SELECT * FROM orders"
items_table_query = "SELECT * FROM items"
customers_table_query = "SELECT * FROM customers"


sales_table = pd.read_sql_query(sales_table_query, conn)
orders_table_raw = pd.read_sql_query(orders_table_query, conn)
items_table = pd.read_sql_query(items_table_query, conn)
customers_table = pd.read_sql_query(customers_table_query, conn)



orders_table = orders_table_raw.dropna()
orders_table["quantity"] = orders_table["quantity"].astype(int)


customers_mask = (customers_table['age']>= 18) & (customers_table['age'] <= 35)
customers_filtered = customers_table[customers_mask]


orders_with_sales_table = pd.merge(orders_table, sales_table, on="sales_id", how="inner")


customers_with_orders_and_sales_table = pd.merge(customers_filtered, orders_with_sales_table, on="customer_id", how="inner")


combined_table_with_label = pd.merge(customers_with_orders_and_sales_table, items_table, on="item_id", how="inner")


final_df = combined_table_with_label.groupby(['customer_id','age','item_name'])['quantity'].sum().reset_index()


output_headers = ['Customer','Age','Item','Quantity']

final_df.to_csv('output_pandas.csv',index=False, header=output_headers)


sql_solution_string = "SELECT customers.customer_id, \
            		customers.age, \
            		items.item_name, \
            		sum(orders.quantity) \
                    FROM orders \
                    INNER JOIN sales ON orders.sales_id = sales.sales_id \
                    INNER JOIN customers ON sales.customer_id = customers.customer_id \
                    INNER JOIN items ON orders.item_id = items.item_id \
                    WHERE customers.age >= 18 and customers.age <=35 \
                    GROUP BY customers.customer_id, customers.age, items.item_name \
                    HAVING sum(orders.quantity) > 0"


sql_solution_table = pd.read_sql_query(sql_solution_string, conn)

sql_solution_table.to_csv('output_sql.csv',index=False, header=output_headers)



