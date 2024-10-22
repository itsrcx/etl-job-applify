import boto3
import json
import os
from binascii import unhexlify
from dotenv import load_dotenv

from pyspark.sql import SparkSession

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

load_dotenv()

def fetch_connection_params(table_name, connection_id):
    """Fetch database credentials from a DynamoDB table with decrypted password."""

    auth_tag_hex = os.environ.get("AUTH_TAG_HEX")
    key_hex = os.environ.get("KEY_HEX")
    iv_hex = os.environ.get("IV_HEX")
    dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table(table_name)

    response = table.get_item(Key={"id": connection_id})

    if "Item" in response:
        item = response["Item"]

        db_creds = {
            "host": item["formData"]["mysql"]["host"],
            "port": int(item["formData"]["mysql"]["port"]),
            "database": item["formData"]["mysql"]["database"],
            "username": item["formData"]["mysql"]["username"],
        }

        decrypted_password = decrypt_pass(
            item["formData"]["mysql"]["password"], auth_tag_hex, key_hex, iv_hex
        )

        db_creds["password"] = decrypted_password

        connection_parms = {
            "id": item["id"],
            "connectionName": item["connectionName"],
            "connectionStatus": item["connectionStatus"],
            "lastModified": item["lastModified"],
            "owner": item["owner"],
            "source": item["source"],
            "userName": item["userName"],
            "db_creds": db_creds
        }

        return connection_parms

    else:
        raise Exception(f"No credentials found for Id: {connection_id}")


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
