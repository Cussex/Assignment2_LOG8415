import subprocess
import boto3
import time
import os
from dotenv import load_dotenv
from functions import *
import visualization

# Load AWS credentials from credentials.env
load_dotenv("credentials.env")
aws_access_key_id = os.environ["aws_access_key_id"]
aws_secret_access_key = os.environ["aws_secret_access_key"]
aws_session_token = os.environ["aws_session_token"]

# Define EC2 instance parameters
keyPairName = 'LOG8415E'
securityGroupName = 'LOG8415E_B2'

# Create an EC2 client
EC2 = boto3.client(
    'ec2',
    region_name="us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)

WAITER = EC2.get_waiter('instance_status_ok')

# get vpc_id
vpc_id = EC2.describe_vpcs().get('Vpcs', [{}])[0].get('VpcId', '')
subnet = EC2.describe_subnets().get('Subnets', [{}])[0].get('SubnetId', '')

# create key pair and security group
print('Creating key pair...')
create_key_pair(EC2, keyPairName)
print('Creating security group...')
security_group = create_security_group(EC2, securityGroupName, vpc_id)

# Create 1 m4.large instances
print("Creating instance...")
instance_id = create_m4large_instance(EC2, keyPairName, security_group['GroupId'], subnet)
WAITER = EC2.get_waiter('instance_status_ok')
WAITER.wait(InstanceIds=[instance_id])
print("Instance is running")

# Get instance public IP
response = EC2.describe_instances(InstanceIds=[instance_id])
public_dns = response['Reservations'][0]['Instances'][0]['PublicDnsName']

# Wait for Spark Master UI to be reachable
print("Waiting for Spark Master UI to be reachable...")
while True:
    try:
        subprocess.check_output(["curl", f"{public_dns}:8080"], stderr=subprocess.STDOUT)
        break
    except subprocess.CalledProcessError:
        time.sleep(30)
print("Spark Master UI is reachable at http://" + public_dns + ":8080")

os.system("chmod 700 " + keyPairName + ".pem")

print("Running hadoop and linux wordcount...")
os.system("ssh -o StrictHostKeyChecking=no -i " + keyPairName + ".pem ubuntu@" + public_dns + " 'bash -s' < ./hadoop_linux_wordcount.sh")
time.sleep(30)
os.system("scp -o StrictHostKeyChecking=no -r -i " + keyPairName + ".pem ubuntu@" + public_dns + ":~/results/* ./results_hadoop_linux")
print("Results saved in ./results_hadoop_linux")

print("Running hadoop and spark wordcount...")
os.system("ssh -o StrictHostKeyChecking=no -i " + keyPairName + ".pem ubuntu@" + public_dns + " 'bash -s' < ./hadoop_spark_wordcount.sh")
time.sleep(30)
os.system("scp -o StrictHostKeyChecking=no -r -i " + keyPairName + ".pem ubuntu@" + public_dns + ":~/results/* ./results_hadoop_spark")
print("Results saved in ./results_hadoop_spark")
time.sleep(5)

os.system("mkdir -p visualization")

visualization.main()

input("Press Enter to delete everything...")

print("Terminating instance...")
terminate_instance(EC2, instance_id)
WAITER = EC2.get_waiter('instance_terminated')
WAITER.wait(InstanceIds=[instance_id])
print("Instance terminated")

print("Deleting security group...")
delete_security_group(EC2, securityGroupName)

print("Deleting key pair...")
delete_key_pair(EC2, keyPairName)