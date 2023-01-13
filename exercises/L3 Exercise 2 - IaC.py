#!/usr/bin/env python
# coding: utf-8

# # Exercise 2: Creating Redshift Cluster using the AWS python SDK 
# ## An example of Infrastructure-as-code

# In[2]:


import pandas as pd
import boto3
import json


# # STEP 0: Make sure you have an AWS secret and access key
# 
# - Create a new IAM user in your AWS account
# - Give it `AdministratorAccess`, From `Attach existing policies directly` Tab
# - Take note of the access key and secret 
# - Edit the file `dwh.cfg` in the same folder as this notebook and fill
# <font color='red'>
# <BR>
# [AWS]<BR>
# KEY= YOUR_AWS_KEY<BR>
# SECRET= YOUR_AWS_SECRET<BR>
# <font/>
# 

# # Load DWH Params from a file

# In[3]:


import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

REGION_NAME            = config.get('default', 'REGION_NAME')
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })


# ## Create clients for EC2, S3, IAM, and Redshift

# In[4]:


import boto3

ec2 = boto3.resource('ec2', 
                     region_name=REGION_NAME, 
                     aws_access_key_id=KEY, 
                     aws_secret_access_key=SECRET)

s3 = boto3.resource('s3', 
                    region_name=REGION_NAME, 
                    aws_access_key_id=KEY, 
                    aws_secret_access_key=SECRET)

iam = boto3.client('iam', 
                   region_name=REGION_NAME,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

redshift = boto3.client('redshift', 
                        region_name=REGION_NAME, 
                        aws_access_key_id=KEY, 
                        aws_secret_access_key=SECRET)


# ## Check out the sample data sources on S3

# In[5]:


sampleDbBucket =  s3.Bucket("awssampledbuswest2")

# TODO: Iterate over bucket objects starting with "ssbgz" and print
# for obj in sampleDbBucket.objects.all():
for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
    print(obj)


# ## STEP 1: IAM ROLE
# - Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

# In[7]:


try:
    iam.detach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
    )
    iam.delete_role(
        RoleName=DWH_IAM_ROLE_NAME
    )
except Exception as e:
    print(e)


# In[8]:


# TODO: Create the IAM role
try:
    print('1.1 Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        AssumeRolePolicyDocument= json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": [
                                    "redshift.amazonaws.com"
                                ]
                            },
                            "Action": [
                                "sts:AssumeRole"
                            ]
                        }
                    ]
                }),
        Description='Allows redshift to assume this role and access s3'
    )
except Exception as e:
    print(e)


# In[9]:


# TODO: Attach Policy
try:
    print('1.2 Attaching Policy')
    iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
        )
except Exception as e:
    print(e)


# In[10]:


# TODO: Get and print the IAM role ARN
try:
    print('1.3 Get the IAM role ARN')
    roleArn = dwhRole['Role']['Arn']
    print(roleArn)
except Exception as e:
    print(e)


# ## STEP 2:  Redshift Cluster
# 
# - Create a RedShift Cluster
# - For complete arguments to `create_cluster`, see [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster)

# In[11]:


try:
    response = redshift.create_cluster(        
        # TODO: add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        
        # TODO: add parameters for identifiers & credentials
        DBName=DWH_DB,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        
        # TODO: add parameter for role (to allow s3 access)
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)


# ## 2.1 *Describe* the cluster to see its status
# - run this block several times until the cluster status becomes `Available`

# In[14]:


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)


# <h2> 2.2 Take note of the cluster <font color='red'> endpoint and role ARN </font> </h2>

# <font color='red'>DO NOT RUN THIS unless the cluster status becomes "Available" </font>

# In[16]:


DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


# ## STEP 3: Open an incoming  TCP port to access the cluster ednpoint

# In[18]:


try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,  # TODO: fill out
        CidrIp='0.0.0.0/0',  # TODO: fill out
        IpProtocol='TCP',  # TODO: fill out
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)


# ## STEP 4: Make sure you can connect to the clusterConnect to the cluster

# In[19]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[20]:


conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')


# ## STEP 5: Clean up your resources

# <b><font color='red'>DO NOT RUN THIS UNLESS YOU ARE SURE <br/> 
#     We will be using these resources in the next exercises</span></b>

# In[21]:


#### CAREFUL!!
#-- Uncomment & run to delete the created resources
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
#### CAREFUL!!


# - run this block several times until the cluster really deleted

# In[27]:


myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)


# In[29]:


#### CAREFUL!!
#-- Uncomment & run to delete the created resources
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
#### CAREFUL!!


# In[ ]:




