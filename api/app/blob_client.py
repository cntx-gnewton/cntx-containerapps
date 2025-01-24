from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO
import json
import yaml
import os
from jinja2 import Template
import logging
import fnmatch


class BlobClient:

    def __init__(self, container_name:str):

        self.conn_str = os.getenv("BlobServiceClientConnStr")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.conn_str)
        self._container_name = container_name

    @property
    def container_name(self):
        return self._container_name

    @container_name.setter
    def container_name(self, value):
        self._container_name = value

    def blob_exists(self, blob_name):
        blob_client = self.blob_service_client.get_blob_client(
            self.container_name, blob_name)
        does_blob_exist = blob_client.exists()
        if not does_blob_exist:
            logging.warning(f"Blob {blob_name} does not exist")
        return does_blob_exist

    def list_blobs(self, pattern=None):
        container_client = self.blob_service_client.get_container_client(
            self.container_name)
        blob_list = [blob.name for blob in container_client.list_blobs()]
        if pattern:
            blob_list = [
                blob for blob in blob_list if fnmatch.fnmatch(blob, pattern)]
        return blob_list

    def render_template(self, blob_name,  **kwargs):
        if not self.blob_exists(blob_name):
            return None
        blob_data = self.read(blob_name, self.container_name)

        # Get the file extension
        _, file_extension = os.path.splitext(blob_name)

        # If the file extension is .j2, render the template
        if file_extension == '.j2':
            template = Template(blob_data)
            rendered_template = template.render(**kwargs)
            return rendered_template
            # return yaml.safe_load(rendered_template)
        # Otherwise, return the blob data as is
        else:
            return blob_data

    def read(self, blob_name, smart=True,):
        blob_client = self.blob_service_client.get_blob_client(
            self.container_name, blob_name)
        blob_data = blob_client.download_blob().readall()

        if not smart:
            return blob_data

        # Get the file extension
        _, file_extension = os.path.splitext(blob_name)

        # Load the data as a dictionary if the file extension is .yml, .yaml, .json, or .txt
        if file_extension in ['.yml', '.yaml']:
            blob_str = blob_data.decode('utf-8')
            return yaml.safe_load(blob_str)
        elif file_extension == '.json':
            blob_str = blob_data.decode('utf-8')
            return json.loads(blob_str)
        elif file_extension in ['.txt', '.j2']:
            blob_str = blob_data.decode('utf-8')
            return blob_str
        elif file_extension == '.xlsx':
            data_file = BytesIO(blob_data)
            return pd.read_excel(data_file, engine='openpyxl')
        elif file_extension == '.csv':
            data_file = BytesIO(blob_data)
            return pd.read_csv(data_file)
        elif file_extension == '.parquet':
            data_file = BytesIO(blob_data)
            return pd.read_parquet(data_file)
        else:
            return blob_data

    def write(self, blob_name, data):

        blob_client = self.blob_service_client.get_blob_client(
            self.container_name, blob_name)

        # Get the file extension
        _, file_extension = os.path.splitext(blob_name)

        # Preprocess the data according to its type
        if file_extension in ['.yml', '.yaml']:
            data_str = yaml.dump(data)
        elif file_extension == '.json':
            data_str = json.dumps(data)
        elif file_extension in ['.txt', '.j2']:
            data_str = data
        elif file_extension == '.xlsx':
            data_file = BytesIO()
            data.to_excel(data_file, engine='openpyxl', index=False)
            data_file.seek(0)
            data = data_file.read()
        elif file_extension == '.csv':
            data_file = BytesIO()
            data.to_csv(data_file, index=False)
            data_file.seek(0)
            data = data_file.read()
        elif file_extension == '.parquet':
            data_file = BytesIO()
            data.to_parquet(data_file)
            data_file.seek(0)
            data = data_file.read()
        else:
            data_str = str(data)

        if isinstance(data, str):
            data = data_str.encode('utf-8')
        else:
            data = data_str.encode('utf-8') if data_str else data

        # Upload the blob
        blob_client.upload_blob(data, overwrite=True)

    def delete_blob(self, blob_name):
        blob_client = self.blob_service_client.get_blob_client(
            self.container_name, blob_name)
        blob_client.delete_blob()
