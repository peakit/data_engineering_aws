import configparser
import boto3
import json
import time
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

REDSHIFT_CLIENT = None
IAM_CLIENT = None
EC2_CLIENT = None


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def get_redshift_client(config):
    """
    If redshift client has already been created then returns that.
    Else creates a new, saves it for future use in REDSHIFT_CLIENT constant
    and returns that.
    """
    global REDSHIFT_CLIENT
    if REDSHIFT_CLIENT is None:
        REDSHIFT_CLIENT = boto3.client('redshift',
                                       region_name=config.get(
                                           'AWS', 'REGION_NAME'),
                                       aws_access_key_id=config.get(
                                           'AWS', 'KEY'),
                                       aws_secret_access_key=config.get('AWS', 'SECRET'))
    return REDSHIFT_CLIENT


def describe_redshift(config):
    """
    Plain wrapper over the boto3 describe_clusters API.
    Uses the cluster identifier from the config.
    """
    return get_redshift_client(config).describe_clusters(
        ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))


def is_redshift_available(config, current_status=None):
    """
    Digs into the status of the cluster (if provided) and returns True if
    a cluster matching the cluster identifier needed for the set up is 
    Available. Else returns False. 

    If no current_status is provided then it queries the current status
    using describe_redshift() function
    """
    if current_status is None:
        current_status = describe_redshift(config)

    if current_status is not None:
        clusters = current_status.get('Clusters')
        for cl in clusters:
            if cl.get('ClusterIdentifier') == config.get('CLUSTER', 'CLUSTER_IDENTIFIER'):
                if cl.get('ClusterStatus') == 'Available':
                    return True
                else:
                    return False
    return False


def wait_for_availability(config, retry_interval, timeout):
    """
    Immediately after creation, the cluster won't be available.
    This method waits for the cluster to become available by the
    timeout specified. It periodically checks the cluster status
    after every retry interval.

    Throws TimeoutError if the cluster does not become available
    by the timeout interval.
    """
    t_start = time.time()
    while not is_redshift_available(config) and \
            time.time() - t_start < timeout:
        time.sleep(retry_interval)
    if not is_redshift_available(config):
        raise(TimeoutError("Cluster is not yet available, timedout!"))


def get_ec2_client(config):
    """
    If EC2 client has already been created then returns that.
    Else creates a new, saves it for future use in EC2_CLIENT constant
    and returns that.
    """
    global EC2_CLIENT
    if EC2_CLIENT is None:
        EC2_CLIENT = boto3.resource('ec2',
                                    region_name=config.get(
                                        'AWS', 'REGION_NAME'),
                                    aws_key_id=config.get('AWS', 'KEY'),
                                    aws_secret_access_id=config.get(
                                        'AWS', 'SECRET')
                                    )
    return EC2_CLIENT


def open_tcp_ingress_to_cluster(config, cluster):
    """
    Opens a TCP ingress to the redshift cluster at the port
    specified in the configuration. The cluster parameter
    is used to refer to the JSON response from cluster 
    creation/describe API.
    """

    vpc = get_ec2_client(config).Vpc(id=cluster['Cluster']
                                     ['Endpoint']['VpcEndpoints'][0]['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]

    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(config.get('CLUSTER', 'DB_PORT')),
        ToPort=int(config.get('CLUSTER', 'DB_PORT'))
    )


def get_iam_client(config):
    """
    If IAM client has already been created then returns that.
    Else creates a new, saves it for future use in IAM_CLIENT constant
    and returns that.
    """
    global IAM_CLIENT
    if IAM_CLIENT is None:
        IAM_CLIENT = boto3.client('iam',
                                  region_name=config.get('AWS', 'REGION_NAME'),
                                  aws_access_key_id=config.get('AWS', 'KEY'),
                                  aws_secret_access_key=config.get('AWS', 'SECRET'))
    return IAM_CLIENT


def get_role(config):
    """
    If the role is already created then gets that.
    """
    return get_iam_client(config).get_role(RoleName=config.get('IAM_ROLE', 'DWH_IAM_ROLE_NAME'))


def attach_s3_full_access_policy(config):
    """
    Attaches s3 full access policy to redshift role
    """
    get_iam_client(config).attach_role_policy(RoleName=config.get('IAM_ROLE', 'DWH_IAM_ROLE_NAME'),
                                              PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')


def get_or_create_iam_role(config):
    """
    If the role specified in the config already exists then it fetches that.
    Else creates a new one. 
    The S3 full access policy is also applied to the role.
    """
    role = get_role(config)

    if role is None:
        role = get_iam_client(config).create_role(
            Path='/',
            RoleName=config.get('IAM_ROLE', 'DWH_IAM_ROLE_NAME'),
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": [
                                "redshift.amazonaws.com"
                            ]
                        },
                        "Action": ["sts:AssumeRole"]
                    }
                ]
            }),
            Description='Allows redshift to assume this role and access s3')

    attach_s3_full_access_policy(config)
    return role


def create_redshift_cluster(config, roleArn):
    """
    Creates redshift cluster using the configurations from config
    and role from roleArn.
    """
    resp = get_redshift_client(config).create_cluster(
        ClusterType=config['CLUSTER']['CLUSTER_TYPE'],
        NodeType=config['CLUSTER']['NODE_TYPE'],
        NumberOfNodes=int(config['CLUSTER']['HOST']),
        DBName=config['CLUSTER']['DB_NAME'],
        MasterUsername=config['CLUSTER']['DB_USER'],
        MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],
        ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
        IamRoles=[roleArn])
    return resp


def get_or_create_redshift(config):
    """
    If an existing cluster with the same cluster identifier exists then good 
    else create one afresh
    """
    # 1. describe cluster
    # 2. If cluster exists then make sure its status is Available
    # 3. Else create a new role, and cluster
    # 3.1 Wait for cluster to become Available
    # return response
    # 4 open tcp ingress to the cluster
    try:
        resp = describe_redshift(config)
    except Exception as e:
        role = get_or_create_iam_role(config)
        resp = create_redshift_cluster(config, role['Role']['Arn'])

    if not is_redshift_available(config, resp):
        wait_for_availability(config, retry_interval=30, timeout=600)
    open_tcp_ingress_to_cluster(config, resp)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    redshift_resp = get_or_create_redshift(config)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
