import boto3
import csv

# Zachary Hicks - CS 1660 - Cloud Storage Homework - nosqlhw.py
# Key information has been replaced with ???? for purposes of submission on Github

# set up s3 - for bucket
s3 = boto3.resource('s3', aws_access_key_id = '????', aws_secret_access_key = '????' )

# create bucket if it does not exist
try:
    s3.create_bucket(Bucket='zacharyhicks1', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("Bucket already exists")

# grab bucket, set to public access
bucket = s3.Bucket("zacharyhicks1")
bucket.Acl().put(ACL='public-read')

# TEST PASSED - try to put an object in bucket from working dir (if it is in there already, it will update/replace
#s3.Object('zacharyhicks1', 'second-credits.png').put(Body=open('C:/Users/Zach/Desktop/SENIOR 2020/Cloud Computing/Storage Homework/second-credits.png', 'rb'))

dyndb = boto3.resource('dynamodb', region_name = 'us-west-2', aws_access_key_id = '????', aws_secret_access_key = '????')

# Try to create the table according to these parameters if it does not exist
try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {'AttributeName': 'PartitionKey','KeyType': 'HASH'},
            {'AttributeName': 'RowKey','KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'PartitionKey','AttributeType': 'S'},
            {'AttributeName': 'RowKey','AttributeType': 'S'},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    print("Table already exists")
    table = dyndb.Table("DataTable")

# wait for table to be created (if it has not been already)
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

# now read in data from master.csv, and load the individual files as blobs into bucket
with open('C:/Users/Zach/Desktop/SENIOR 2020/Cloud Computing/Storage Homework/master.csv') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open('C:/Users/Zach/Desktop/SENIOR 2020/Cloud Computing/Storage Homework/data/'+item[3], 'rb')
        s3.Object('zacharyhicks1', item[3]).put(Body=body)
        md = s3.Object('zacharyhicks1', item[3]).Acl().put(ACL='public-read')
        
        url = "https://s3-us-west-2.amazonws.com/zacharyhicks1/"+item[3]
        metadata_item = {
            'PartitionKey': item[0],
            'RowKey': item[1],
            'description': item[4],
            'date': item[2],
            'url': url
        }
        try:
            table.put_item(Item=metadata_item)
        except:
            print("This item may already be there")

# Test that we can query the table for these items
response = table.get_item(
    Key={
        'PartitionKey': 'Experiment2',
        'RowKey': '2'
    }
)

# print out the response - the full set of data from table we queried
print('\nQuery for Experiment2\n')
print(response)