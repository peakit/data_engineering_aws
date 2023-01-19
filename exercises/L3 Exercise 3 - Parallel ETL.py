#!/usr/bin/env python
# coding: utf-8

# # Exercise 3: Parallel ETL

# In[34]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[35]:


import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time


# # STEP 1: Get the params of the created redshift cluster 
# - We need:
#     - The redshift cluster <font color='red'>endpoint</font>
#     - The <font color='red'>IAM role ARN</font> that give access to Redshift to read from S3

# In[36]:


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

REGION_NAME = config.get('default', 'region_name')
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")


# In[37]:


# FILL IN THE REDSHIFT ENPOINT HERE
# e.g. DWH_ENDPOINT="redshift-cluster-1.csmamz5zxmle.us-west-2.redshift.amazonaws.com" 
DWH_ENDPOINT="dwhcluster.ceok3cz80k9u.us-west-2.redshift.amazonaws.com" 
    
#FILL IN THE IAM ROLE ARN you got in step 2.2 of the previous exercise
#e.g DWH_ROLE_ARN="arn:aws:iam::988332130976:role/dwhRole"
DWH_ROLE_ARN="arn:aws:iam::996813506119:role/dwhRole"


# # STEP 2: Connect to the Redshift Cluster

# In[40]:


conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')


# In[41]:


print(REGION_NAME)


# In[43]:


s3 =  boto3.resource('s3', region_name=REGION_NAME,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

sampleDbBucket =  s3.Bucket('udacity-labs')


# In[44]:


for obj in sampleDbBucket.objects.filter(Prefix="tickets"):
    print(obj)


# # STEP 3: Create Tables

# In[46]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS "sporting_event_ticket";\nCREATE TABLE "sporting_event_ticket" (\n    "id" double precision DEFAULT nextval(\'sporting_event_ticket_seq\') NOT NULL,\n    "sporting_event_id" double precision NOT NULL,\n    "sport_location_id" double precision NOT NULL,\n    "seat_level" numeric(1,0) NOT NULL,\n    "seat_section" character varying(15) NOT NULL,\n    "seat_row" character varying(10) NOT NULL,\n    "seat" character varying(10) NOT NULL,\n    "ticketholder_id" double precision,\n    "ticket_price" numeric(8,2) NOT NULL\n);')


# # STEP 4: Load Partitioned data into the cluster
# Use the COPY command to load data from `s3://udacity-labs/tickets/split/part` using your iam role credentials. Use gzip delimiter `;`.

# In[48]:


get_ipython().run_cell_magic('time', '', 'qry = """\n    COPY sporting_event_ticket FROM \'s3://udacity-labs/tickets/split/part\'\n    CREDENTIALS \'aws_iam_role={}\'\n    gzip delimiter \';\' compupdate off region \'us-west-2\';\n""".format(DWH_ROLE_ARN)\n\n%sql $qry')


# # STEP 5: Create Tables for the non-partitioned data

# In[49]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS "sporting_event_ticket_full";\nCREATE TABLE "sporting_event_ticket_full" (\n    "id" double precision DEFAULT nextval(\'sporting_event_ticket_seq\') NOT NULL,\n    "sporting_event_id" double precision NOT NULL,\n    "sport_location_id" double precision NOT NULL,\n    "seat_level" numeric(1,0) NOT NULL,\n    "seat_section" character varying(15) NOT NULL,\n    "seat_row" character varying(10) NOT NULL,\n    "seat" character varying(10) NOT NULL,\n    "ticketholder_id" double precision,\n    "ticket_price" numeric(8,2) NOT NULL\n);')


# # STEP 6: Load non-partitioned data into the cluster
# Use the COPY command to load data from `s3://udacity-labs/tickets/full/full.csv.gz` using your iam role credentials. Use gzip delimiter `;`.
# 
# - Note how it's slower than loading partitioned data

# In[50]:


get_ipython().run_cell_magic('time', '', '\nqry = """\n    COPY sporting_event_ticket_full FROM \'s3://udacity-labs/tickets/full/full.csv.gz\'\n    CREDENTIALS \'aws_iam_role={}\'\n    gzip delimiter \';\' compupdate off region \'us-west-2\';\n""".format(DWH_ROLE_ARN)\n\n%sql $qry')


# In[ ]:




