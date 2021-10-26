import boto3
import csv as csv

def bucket():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id='*************',
        aws_secret_access_key='******************'
    )


    try:
        s3.create_bucket(Bucket='cs1660.atr34', CreateBucketConfiguration={
            'LocationConstraint': 'us-east-2'})
    except Exception as e:
        print(e)

    #we will make this bucket publicly readable
    bucket = s3.Bucket("cs1660.atr34")
    bucket.Acl().put(ACL='public-read')


    #Create the dynamodb database
    dyndb = boto3.resource('dynamodb',
        region_name='us-east-2',
        aws_access_key_id='*******************************',
        aws_secret_access_key='*****************************'
    )  

    try:
        table = dyndb.create_table(
            TableName='DataTable',
            KeySchema = 
            [
                {
                    'AttributeName': 'PartitionKey',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'RowKey',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions = 
            [
                {
                    'AttributeName': 'PartitionKey',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'RowKey',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput = 
            {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except Exception as e:
        print (e)  

        #if there is an exception, the table may already exist. if so...
        table = dyndb.Table("DataTable")

    #wait for the table to be created
    table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')    
    print(table.item_count)

    with open('experiments.csv', 'r') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
        for item in csvf: 
            print(item) 
            if item[4].__contains__('.csv') == True:
                body = open(item[4], 'rb')            
            else:
                continue
            print('body')
            s3.Object('datacont-name', item[4]).put(Body=body)
            md = s3.Object('datacont-name', item[4]).Acl().put(ACL='public-write')
            url = " https://s3-us-east-2.amazonaws.com/datacont-name/"+item[3]
            metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                'description' : item[4], 'date' : item[2], 'url':url}

            try:
                table.put_item(Item=metadata_item)
            except:
                print("item may already be there or another failure")

bucket()

print('success')
