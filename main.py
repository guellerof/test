import boto3
from botocore.exceptions import ClientError
import json
from multiprocessing import Pool
import argparse
import datetime
from dateutil.relativedelta import relativedelta

def read_args():
    """
    This function reads the args from terminal 
    """

    parser = argparse.ArgumentParser(description='This script extracts all the s3 buckets')

    # Optional argument
    parser.add_argument('--regions',help='Filter regions to read buckets - e.g.: "us-east-1,us-east-2"')
    parser.add_argument('--life-cycle',action='store_true', help='Boolean parameter to filter only buckets that have a life cycle rule')
    parser.add_argument('--prefix',help='Prefix to filter only some files inside the bucket - e.g.: "logs/"')

    args = parser.parse_args()

    regions = list()
    if args.regions:
        regions = args.regions.split(',')

    if not args.prefix:
        args.prefix = ""

    return {'regions': regions, 'prefix': args.prefix, 'life_cycle':args.life_cycle}

args = read_args()
client = boto3.client('s3')

def sum_objects(objs):
    last_modified = None
    total_size = 0
    files = 0

    if args['regions']:
        if objs['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region'] not in args['regions']:
            return None

    if 'Contents' in objs:
        files = len(objs['Contents'])
        for obj in objs['Contents']:
            if not last_modified:
                last_modified = obj['LastModified']
            elif obj['LastModified']>last_modified:
                last_modified = obj['LastModified']
            
            total_size += obj['Size']

    if last_modified:
        last_modified = last_modified.strftime("%d-%m-%Y %H:%M:%S")
    else:
        last_modified = "-"

    return {'Last_Modified': last_modified, 'Files': files,'Size': total_size, 'Region': objs['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']}

def get_bucket_files(bucket):
    details = dict()
    if args['life_cycle']:
        try:
            lifecyle_rule = client.get_bucket_lifecycle_configuration(Bucket=bucket['Name'])
        except ClientError as e:
            if (e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration'):
                return None
    details['Name'] = bucket['Name']
    details['Creation_Date'] = bucket['CreationDate'].strftime("%d-%m-%Y %H:%M:%S")
    objs = client.list_objects(Bucket = bucket['Name'],Prefix = args['prefix'])
    objects_details = sum_objects(objs)
    if objects_details:
        details.update(objects_details)
        return details
    return None 
    
def print_table(table,cost):
    # Format args
    if not args['prefix']:
        prefix = "Not Applied"
    else:
        prefix = args['prefix']

    if not args['regions']:
        regions = "Not Applied"
    else:
        regions = ','.join(args['regions'])

    if not args['life_cycle']:
        life_cycle = "Not Applied"
    else:
        life_cycle = "Applied"

    print("|----------------------------------------------------------------------- Header -----------------------------------------------------------------------|")
    print("|Filters:  Regions: {:^56} | Life Cycle: {:^11} | Prefix: {:^37} |".format(regions,life_cycle,prefix))
    print("|Buckets Found:  {:<59} | S3 total cost in last month: {:<43}|".format(len(table),cost))
              
    print("|------------------------------------------------------------------------------------------------------------------------------------------------------|")

    template = "|{Name:64}|{Region:^14}|{Creation_Date:^21}|{Last_Modified:^21}|{Files:^15}|{Size:^10}|" # same, but named
    print (template.format(Name="Bucket Name",Region = "Region", Creation_Date="Creation Date (UTC)", Files = "Number of Files",Last_Modified="Last Modified (UTC)", Size="Size (B)")) 
    print("|------------------------------------------------------------------------------------------------------------------------------------------------------|")

    for rec in table:
        print(template.format(**rec))
    print("|------------------------------------------------------------------------------------------------------------------------------------------------------|")

def get_cost_data():
    client = boto3.client('ce')
    today = datetime.datetime.now()
    end = today.strftime('%Y-%m-%d')
    start = today+relativedelta(months=-1)
    start = start.strftime('%Y-%m-%d')
    response = client.get_cost_and_usage(
        TimePeriod={
                'Start': start, 
                'End':  end
            }, 
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
    )

    cost = float(0)
    unit = ""
    for r in response['ResultsByTime']:
        for group in r['Groups']:
            if "Amazon Simple Storage Service" in group['Keys']:
                cost += float(group['Metrics']['UnblendedCost']['Amount'])
                unit = group['Metrics']['UnblendedCost']['Unit']

    return str(cost)+" "+unit


def main():
    """
    Main Function 
    """
    cost = get_cost_data()

    response = client.list_buckets()

    p = Pool(50)
    results = p.map(get_bucket_files, response['Buckets'])
    p.terminate()

    results = [x for x in results if x is not None]
    print_table(results,cost)

if __name__ == "__main__":
    main()
