from __future__ import annotations

import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


def init_azure_client(
    env_path: Path,
    container_name: str = "market-data",
) -> tuple[BlobServiceClient, str]:
    load_dotenv(dotenv_path=env_path)

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not connection_string:
        raise ValueError(
            "Missing AZURE_STORAGE_CONNECTION_STRING in .env file."
        )

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )

    container_client = blob_service_client.get_container_client(
        container_name
    )

    if not container_client.exists():
        raise ValueError(f"Azure container does not exist: {container_name}")

    return blob_service_client, container_name


def upload_file(
    blob_service_client: BlobServiceClient,
    container_name: str,
    local_path: Path,
    blob_name: str,
    overwrite: bool = True,
) -> None:
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    with open(local_path, "rb") as file:
        blob_client.upload_blob(
            file,
            overwrite=overwrite,
        )

def blob_exists(
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
) -> bool:
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    return blob_client.exists()