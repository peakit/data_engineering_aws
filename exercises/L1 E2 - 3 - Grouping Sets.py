#!/usr/bin/env python
# coding: utf-8

# # Exercise 02 -  OLAP Cubes - Grouping Sets

# Start by connecting to the database by running the cells below. If you are coming back to this exercise, then uncomment and run the first cell to recreate the database. If you recently completed the slicing and dicing exercise, then skip to the second cell.

# In[ ]:


# !PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila_star
# !PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila_star -f Data/pagila-star.sql


# ### Connect to the local database where Pagila is loaded

# In[1]:


import sql
get_ipython().run_line_magic('load_ext', 'sql')

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila_star'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}"                         .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')


# ### Star Schema

# <img src="pagila-star.png" width="50%"/>

# # Grouping Sets
# - It happens often that for 3 dimensions, you want to aggregate a fact:
#     - by nothing (total)
#     - then by the 1st dimension
#     - then by the 2nd 
#     - then by the 3rd 
#     - then by the 1st and 2nd
#     - then by the 2nd and 3rd
#     - then by the 1st and 3rd
#     - then by the 1st and 2nd and 3rd
#     
# - Since this is very common, and in all cases, we are iterating through all the fact table anyhow, there is a more clever way to do that using the SQL grouping statement "GROUPING SETS" 

# ## Total Revenue
# 
# TODO: Write a query that calculates total revenue (sales_amount)

# In[3]:


get_ipython().run_cell_magic('sql', '', 'SELECT SUM(sales_amount) AS "total revenue"\nFROM factsales')


# ## Revenue by Country
# TODO: Write a query that calculates total revenue (sales_amount) by country

# In[5]:


get_ipython().run_cell_magic('sql', '', 'SELECT country, SUM(sales_amount) as revenue\nFROM factsales f\nJOIN dimcustomer c\n  ON f.customer_key = c.customer_key\nGROUP BY c.country')


# ## Revenue by Month
# TODO: Write a query that calculates total revenue (sales_amount) by month

# In[6]:


get_ipython().run_cell_magic('sql', '', 'SELECT month, SUM(sales_amount) AS revenue\nFROM factsales f\nJOIN dimdate d\n  ON f.date_key = d.date_key\nGROUP BY month')


# ## Revenue by Month & Country
# TODO: Write a query that calculates total revenue (sales_amount) by month and country. Sort the data by month, country, and revenue in descending order. The first few rows of your output should match the table below.

# In[9]:


get_ipython().run_cell_magic('sql', '', 'SELECT month, country, SUM(sales_amount) AS revenue\nFROM factsales f\nJOIN dimdate d\n  ON f.date_key = d.date_key\nJOIN dimstore s\n  ON f.store_key = s.store_key\nGROUP BY month, country\nORDER BY month, country, revenue DESC')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>month</th>
#         <th>country</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>Australia</td>
#         <td>2364.19</td>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>Canada</td>
#         <td>2460.24</td>
#     </tr>
#     <tr>
#         <td>2</td>
#         <td>Australia</td>
#         <td>4895.10</td>
#     </tr>
#     <tr>
#         <td>2</td>
#         <td>Canada</td>
#         <td>4736.78</td>
#     </tr>
#     <tr>
#         <td>3</td>
#         <td>Australia</td>
#         <td>12060.33</td>
#     </tr>
# </tbody></table></div>

# ## Revenue Total, by Month, by Country, by Month & Country All in one shot
# 
# TODO: Write a query that calculates total revenue at the various grouping levels done above (total, by month, by country, by month & country) all at once using the grouping sets function. Your output should match the table below.

# In[16]:


get_ipython().run_cell_magic('sql', '', 'SELECT d.month, s.country, sum(sales_amount) AS revenue\nFROM factsales f\nJOIN dimstore s\n  ON f.store_key = s.store_key\nJOIN dimdate d\n  ON d.date_key = f.date_key\nGROUP BY GROUPING SETS (d.month, s.country, (), (d.month, s.country))\nORDER BY d.month, s.country, revenue DESC')


# <div class="p-Widget jp-RenderedHTMLCommon jp-RenderedHTML jp-mod-trusted jp-OutputArea-output jp-OutputArea-executeResult" data-mime-type="text/html"><table>
#     <tbody><tr>
#         <th>month</th>
#         <th>country</th>
#         <th>revenue</th>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>Australia</td>
#         <td>2364.19</td>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>Canada</td>
#         <td>2460.24</td>
#     </tr>
#     <tr>
#         <td>1</td>
#         <td>None</td>
#         <td>4824.43</td>
#     </tr>
#     <tr>
#         <td>2</td>
#         <td>Australia</td>
#         <td>4895.10</td>
#     </tr>
#     <tr>
#         <td>2</td>
#         <td>Canada</td>
#         <td>4736.78</td>
#     </tr>
#     <tr>
#         <td>2</td>
#         <td>None</td>
#         <td>9631.88</td>
#     </tr>
#     <tr>
#         <td>3</td>
#         <td>Australia</td>
#         <td>12060.33</td>
#     </tr>
#     <tr>
#         <td>3</td>
#         <td>Canada</td>
#         <td>11826.23</td>
#     </tr>
#     <tr>
#         <td>3</td>
#         <td>None</td>
#         <td>23886.56</td>
#     </tr>
#     <tr>
#         <td>4</td>
#         <td>Australia</td>
#         <td>14136.07</td>
#     </tr>
#     <tr>
#         <td>4</td>
#         <td>Canada</td>
#         <td>14423.39</td>
#     </tr>
#     <tr>
#         <td>4</td>
#         <td>None</td>
#         <td>28559.46</td>
#     </tr>
#     <tr>
#         <td>5</td>
#         <td>Australia</td>
#         <td>271.08</td>
#     </tr>
#     <tr>
#         <td>5</td>
#         <td>Canada</td>
#         <td>243.10</td>
#     </tr>
#     <tr>
#         <td>5</td>
#         <td>None</td>
#         <td>514.18</td>
#     </tr>
#     <tr>
#         <td>None</td>
#         <td>None</td>
#         <td>67416.51</td>
#     </tr>
#     <tr>
#         <td>None</td>
#         <td>Australia</td>
#         <td>33726.77</td>
#     </tr>
#     <tr>
#         <td>None</td>
#         <td>Canada</td>
#         <td>33689.74</td>
#     </tr>
# </tbody></table></div>
