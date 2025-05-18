import boto3
from botocore.exceptions import ClientError

def get_secret(secret_name, region_name='us-west-2'):
    full_secret_name = 'airflow/variables/' + secret_name

    # Create a Secrets Manager client
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=full_secret_name
        )
        # Secrets Manager decrypts the secret value using the associated KMS key
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = get_secret_value_response['SecretBinary']

        return secret
    except ClientError as e:
        print(f"AWS ClientError {e.response['Error']['Code']}: {e}")
        raise e
