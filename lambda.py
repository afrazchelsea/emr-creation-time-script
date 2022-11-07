import json
import boto3
import datetime
from datetime import timedelta
import os
import sys
import subprocess

# pip install custom package to /tmp/ and add to path
subprocess.call('pip install tabulate -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
sys.path.insert(1, '/tmp/')

from tabulate import tabulate

def lambda_handler(event, context):
    new_clusters = []
    long_running_clusters = []
    emr_client = boto3.client('emr')

    # clusters = emr_client.list_clusters(ClusterStates=['RUNNING'])
    clusters = event
    print("Clusters---->", clusters)

    for cluster in clusters['Clusters']:
        cluster_data=[]
        cluster_id = cluster['Id']
        print("Cluster ID - ", cluster_id)
        cluster_launch_time = str(cluster['Status']['Timeline']['CreationDateTime']).split("+")[0].split(".")[0]
        creation_date = cluster_launch_time.split(" ")[0]
        print("Cluster Launch Time - ", cluster_launch_time)
        elapsed_time = calc_elapsed_time(cluster_launch_time)
        print("Elapsed Time - ", elapsed_time)
        cluster_data.append(cluster_id)
        cluster_data.append(creation_date)
        cluster_data.append(int(elapsed_time))
        if int(elapsed_time) > 24:
            long_running_clusters.append(cluster_data)
        else:
            new_clusters.append(cluster_data)    
            
    print("Long Running - ", long_running_clusters)
    print("New Ones - ", new_clusters)
    
    print("***Long Running Clusters***")
    tabulate_data(long_running_clusters)
    print("***Newly Created Clusters***")
    tabulate_data(new_clusters)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Execution Completed Successfully!')
    }


def utc_now():
    curr_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return datetime.datetime.strptime(curr_time, "%Y-%m-%d %H:%M:%S")


def calc_elapsed_time(launch_time):
    print("---In calc_elapsed_time---")
    launch_dt = datetime.datetime.strptime(launch_time, "%Y-%m-%d %H:%M:%S")
    print(launch_dt)
    elapsed_time = utc_now() - launch_dt
    print(elapsed_time)
    diff_minutes = (elapsed_time.days * 24 * 60) + (elapsed_time.seconds/60)
    print(diff_minutes)
    return diff_minutes/60
    
def tabulate_data(data_list):
    head = ["EMR_ID", "Creation_Data", "Elapsed_Time"]
    print(tabulate(data_list, headers=head, tablefmt="grid"))