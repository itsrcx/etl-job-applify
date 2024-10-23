import json
import os
import boto3
from boto3.dynamodb.conditions import Attr
from binascii import unhexlify
from dotenv import load_dotenv

from pyspark.sql import SparkSession

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

load_dotenv()

def fetch_connection_params(table_name, connection_id):
    """Fetch database credentials from a DynamoDB table with decrypted password."""

    dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table(table_name)

    response = table.get_item(Key={"id": connection_id})

    if "Item" in response:
        connection_params = response["Item"]

        return connection_params

    else:
        raise Exception(f"No data found for Id: {connection_id}")

def get_db_creds(connection_params, source):

    key_hex = os.getenv("KEY_HEX")

    db_creds = {
                "host": connection_params["formData"][f"{source}"]["host"],
                "port": int(connection_params["formData"][f"{source}"]["port"]),
                "database": connection_params["formData"][f"{source}"]["database"],
                "username": connection_params["formData"][f"{source}"]["username"],
            }

    auth_tag_hex = connection_params["formData"][f"{source}"]["authTag"]
    iv_hex = connection_params["formData"][f"{source}"]["passIV"]

    decrypted_password = decrypt_pass(
        connection_params["formData"][f"{source}"]["password"], auth_tag_hex, key_hex, iv_hex
    )

    db_creds["password"] = decrypted_password

    return db_creds

def decrypt_pass(encrypted_data_hex, auth_tag_hex, key_hex, iv_hex):
    try:
        # Convert hex strings to bytes
        encrypted_data = unhexlify(encrypted_data_hex)
        auth_tag = unhexlify(auth_tag_hex)
        key = unhexlify(key_hex)  # 32-byte key
        iv = unhexlify(iv_hex)  # Initialization vector

        # Create the Cipher object
        cipher = Cipher(
            algorithms.AES(key), modes.GCM(iv, auth_tag), backend=default_backend()
        )
        decryptor = cipher.decryptor()

        # Decrypt the data
        decrypted = decryptor.update(encrypted_data) + decryptor.finalize()

        # Return the decrypted JSON object
        return json.loads(decrypted.decode("utf-8"))
    except Exception as error:
        raise error

def fetch_model_mapping(table_name, connection_id):

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    response = table.scan(
        FilterExpression=Attr('connectorId').eq(connection_id)
    )
    if "Items" in response and response["Items"]:
        model_mappings = response["Items"]
        return model_mappings
    else:
        raise Exception(f"No data found for connectorId: {connection_id}")
