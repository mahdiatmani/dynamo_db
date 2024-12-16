import boto3

def DBconnection():
    dynamodb = boto3.resource(
        'dynamodb',
        region_name='eu-north-1',  # Your region
        aws_access_key_id='____________________',       # Replace this
        aws_secret_access_key='__________________'    # Replace this
    )
    return dynamodb 


table = DBconnection().Table('Users')
print(f"Connection Successful! , {table}")