#!/usr/bin/env python
# coding: utf-8

# # STEP 5: ETL the data from 3NF tables to Facts & Dimension Tables
# **IMPORTANT:** The following exercise depends on first having successing completed Exercise 1: Step 4. 
# 
# Start by running the code in the cell below to connect to the database. If you are coming back to this exercise, then uncomment and run the first cell to recreate the database. If you recently completed steps 1 through 4, then skip to the second cell.

# In[2]:


get_ipython().system('PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql')
get_ipython().system('PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql')


# In[3]:


get_ipython().run_line_magic('load_ext', 'sql')

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}"                         .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')


# ### Introducing SQL to SQL ETL
# When writing SQL to SQL ETL, you first create a table then use the INSERT and SELECT statements together to populate the table. Here's a simple example.

# First, you create a table called test_table.

# In[4]:


get_ipython().run_cell_magic('sql', '', 'CREATE TABLE test_table\n(\n  date timestamp,\n  revenue  decimal(5,2)\n);')


# Then you use the INSERT and SELECT statements to populate the table. In this case, the SELECT statement extracts data from the `payment` table and INSERTs it INTO the `test_table`.

# In[5]:


get_ipython().run_cell_magic('sql', '', 'INSERT INTO test_table (date, revenue)\nSELECT payment_date AS date,\n       amount AS revenue\nFROM payment;')


# Then you can use a SELECT statement to take a look at your new table.

# In[6]:


get_ipython().run_line_magic('sql', 'SELECT * FROM test_table LIMIT 5;')


# If you need to delete the table and start over, use the DROP TABLE command, like below.

# In[7]:


get_ipython().run_line_magic('sql', 'DROP TABLE test_table')


# Great! Now you'll do the same thing below to create the dimension and fact tables for the Star Schema using the data in the 3NF database.
# 
# ## ETL from 3NF to Star Schema

# ### 3NF - Entity Relationship Diagram
# 
# <img src="./pagila-3nf.png" width="50%"/>
# 
# ### Star Schema - Entity Relationship Diagram
# 
# <img src="pagila-star.png" width="50%"/>

# In this section, you'll populate the tables in the Star schema. You'll `extract` data from the normalized database, `transform` it, and `load` it into the new tables. 
# 
# To serve as an example, below is the query that populates the `dimDate` table with data from the `payment` table.
# * NOTE 1: The EXTRACT function extracts date parts from the payment_date variable.
# * NOTE 2: If you get an error that says that the `dimDate` table doesn't exist, then go back to Exercise 1: Step 4 and recreate the tables.

# In[9]:


get_ipython().run_cell_magic('sql', '', "INSERT INTO dimDate (date_key, date, year, quarter, month, day, week, is_weekend)\nSELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,\n       date(payment_date)                                           AS date,\n       EXTRACT(year FROM payment_date)                              AS year,\n       EXTRACT(quarter FROM payment_date)                           AS quarter,\n       EXTRACT(month FROM payment_date)                             AS month,\n       EXTRACT(day FROM payment_date)                               AS day,\n       EXTRACT(week FROM payment_date)                              AS week,\n       CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend\nFROM payment;")


# TODO: Now it's your turn. Populate the `dimCustomer` table with data from the `customer`, `address`, `city`, and `country` tables. Use the starter code as a guide.

# In[22]:


get_ipython().run_cell_magic('sql', '', 'DELETE FROM dimCustomer')


# In[23]:


get_ipython().run_cell_magic('sql', '', 'INSERT INTO dimCustomer (customer_key, customer_id, first_name, last_name, email, address, \n                         address2, district, city, country, postal_code, phone, active, \n                         create_date, start_date, end_date)\nSELECT c.customer_id, c.customer_id, c.first_name, c.last_name, \n       c.email, a.address, a.address2, a.district, \n       ci.city, co.country, a.postal_code, \n       a.phone, c.active, c.create_date,\n       now()         AS start_date,\n       now()         AS end_date\nFROM customer c\nJOIN address a  ON (c.address_id = a.address_id)\nJOIN city ci    ON (a.city_id = ci.city_id)\nJOIN country co ON (ci.country_id = co.country_id);')


# TODO: Populate the `dimMovie` table with data from the `film` and `language` tables. Use the starter code as a guide.

# In[17]:


get_ipython().run_cell_magic('sql', '', 'INSERT INTO dimMovie (movie_key, film_id, title, \n                      description, release_year, \n                      language, original_language,\n                      rental_duration, length,\n                      rating, special_features\n                     )\nSELECT f.film_id as movie_key, f.film_id, f.title,\n        f.description, f.release_year,\n        l.name AS language, orig_lang.name AS original_language,\n        f.rental_duration, f.length, f.rating, f.special_features\nFROM film f\nJOIN language l              ON (f.language_id=l.language_id)\nLEFT JOIN language orig_lang ON (f.original_language_id = orig_lang.language_id);')


# TODO: Populate the `dimStore` table with data from the `store`, `staff`, `address`, `city`, and `country` tables. This time, there's no guide. You should write the query from scratch. Use the previous queries as a reference.

# In[20]:


get_ipython().run_cell_magic('sql', '', 'INSERT INTO dimstore(store_key, store_id, address, address2, \n                     district, city, country, postal_code, \n                     manager_first_name, manager_last_name,\n                     start_date, end_date)\nSELECT s.store_id, s.store_id, a.address, a.address2,\n        a.district, ci.city, c.country, a.postal_code,\n        sf.first_name, sf.last_name, now(), now()\n  FROM store s\n  JOIN address a\n    ON s.address_id = a.address_id\n  JOIN city ci\n    ON a.city_id = ci.city_id\n  JOIN country c\n    ON ci.country_id = c.country_id\n  JOIN staff sf\n    ON s.manager_staff_id = sf.staff_id')


# TODO: Populate the `factSales` table with data from the `payment`, `rental`, and `inventory` tables. This time, there's no guide. You should write the query from scratch. Use the previous queries as a reference.

# In[24]:


get_ipython().run_cell_magic('sql', '', "INSERT INTO factSales(date_key, customer_key, movie_key, store_key, sales_amount)\nSELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer),\n        p.customer_id, film_id, store_id, p.amount\n  FROM payment p\n  JOIN rental r\n    ON p.rental_id = r.rental_id\n  JOIN inventory i\n    ON r.inventory_id = i.inventory_id")


# In[ ]:




