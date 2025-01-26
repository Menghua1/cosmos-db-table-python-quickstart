from typing import Optional
from dotenv import load_dotenv

from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from azure.identity import DefaultAzureCredential
from pydantic_settings import BaseSettings
from azure.keyvault.secrets import SecretClient

import json
import os

def getLastRequestCharge(c):
    return c.client_connection.last_response_headers["x-ms-request-charge"]

def keyvault_name_as_attr(name: str) -> str:
    return name.replace("-", "_").upper()

class Settings(BaseSettings):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load secrets from keyvault
        if self.AZURE_KEY_VAULT_ENDPOINT:
            credential = DefaultAzureCredential()
            keyvault_client = SecretClient(self.AZURE_KEY_VAULT_ENDPOINT, credential)
            for secret in keyvault_client.list_properties_of_secrets():
                setattr(
                    self,
                    keyvault_name_as_attr(secret.name),
                    keyvault_client.get_secret(secret.name).value,
                )

    KEY_VAULT_SECRET_AZURE_COSMOS_DB_TABLE_KEY: str = ""
    AZURE_KEY_VAULT_ENDPOINT: Optional[str] = None

def runDemo(writeOutput):
    load_dotenv()

    # <create_client>
    accountName = os.getenv("CONFIGURATION__AZURECOSMOSDB__ACCOUNTNAME")
    if not accountName:
        raise EnvironmentError("Azure Cosmos DB for Table account name not set.")

    endpoint = os.getenv("CONFIGURATION__AZURECOSMOSDB__ENDPOINT")
    if not endpoint:
        raise EnvironmentError("Azure Cosmos DB for Table account endpoint not set.")

    settings = Settings()  
    key = settings.KEY_VAULT_SECRET_AZURE_COSMOS_DB_TABLE_KEY
    if not key:
        raise EnvironmentError("Azure Cosmos DB for Table write key not set.")

    credential = AzureNamedKeyCredential(accountName, key)

    client = TableServiceClient(endpoint=endpoint, credential=credential)
    # </create_client>

    tableName = os.getenv("CONFIGURATION__AZURECOSMOSDB__TABLENAME", "cosmicworks-products")
    table = client.get_table_client(tableName)

    writeOutput(f"Get table:\t{table.table_name}")

    new_entity = {
        "RowKey": "aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb",
        "PartitionKey": "gear-surf-surfboards",
        "Name": "Yamba Surfboard",
        "Quantity": 12,
        "Sale": False,
    }
    created_entity = table.upsert_entity(new_entity)

    writeOutput(f"Upserted entity:\t{created_entity}")

    new_entity = {
        "RowKey": "bbbbbbbb-1111-2222-3333-cccccccccccc",
        "PartitionKey": "gear-surf-surfboards",
        "Name": "Kiama Classic Surfboard",
        "Quantity": 4,
        "Sale": True,
    }
    created_entity = table.upsert_entity(new_entity)
    writeOutput(f"Upserted entity:\t{created_entity}")

    existing_entity = table.get_entity(
        row_key="aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb",
        partition_key="gear-surf-surfboards",
    )

    writeOutput(f"Read entity id:\t{existing_entity['RowKey']}")
    writeOutput(f"Read entity:\t{existing_entity}")

    category = "gear-surf-surfboards"
    filter = f"PartitionKey eq '{category}'"
    entities = table.query_entities(query_filter=filter)

    result = []
    for entity in entities:
        result.append(entity)

    output = json.dumps(result, indent=True)

    writeOutput("Found entities:")
    writeOutput(output, isCode=True)
