import base64
import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import io
import os
import requests
# from blob_client import BlobClient
import pandas as pd
from azure.storage.blob import BlobServiceClient
# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Layout of the app
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("File Upload and API Trigger Example",
                className="text-center mt-4 mb-4"))
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Input(
                id='container-name',
                type='text',
                placeholder='Enter a container name',
                className='mb-3'
            ),
            dbc.Input(
                id='blob-name',
                type='text',
                placeholder='Enter a blob name',
                className='mb-3'
            ),
            dcc.Upload(
                id='upload-data',
                children=html.Div([
                    'Drag and Drop or ',
                    html.A('Select a File')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=False  # Allow only one file to be uploaded at a time
            ),
            html.Button("Upload", id="upload-button", n_clicks=0,
                        className="btn btn-primary mt-2"),
            html.Div(id='output-data-upload')
        ])
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Input(
                id='api-url',
                type='text',
                placeholder='Enter API URL',
                className='mb-3'
            ),
            html.Button("Trigger API", id="api-trigger-button",
                        n_clicks=0, className="btn btn-secondary mt-2"),
            html.Div(id='output-api-trigger')
        ])
    ])
])

# Callback to handle file upload


@app.callback(
    Output('output-data-upload', 'children'),
    Input('upload-button', 'n_clicks'),
    State('upload-data', 'contents'),
    State('container-name', 'value'),
    State('blob-name', 'value'),
    State('upload-data', 'filename')
)
def update_output(n_clicks, contents, container_name, blob_name, filename):
    if n_clicks == 0:
        return ""

    if not container_name:
        return html.Div("Please specify a container name before uploading.")

    if not blob_name:
        return html.Div("Please specify a blob name before uploading.")

    if contents is not None:
        try:
            blob_service_client = BlobServiceClient.from_connection_string(
                os.getenv("BlobServiceClientConnStr"))
            blob_client = blob_service_client.get_blob_client(
                container_name, blob_name)
            content_type, content_string = contents.split(',')
            decoded = base64.b64decode(content_string)
            # Determine if the file is CSV or Excel and convert to bytes
            if filename.endswith('.csv'):
                data = io.BytesIO(decoded)  # Already in bytes
            elif filename.endswith(('.xls', '.xlsx')):
                excel_data = io.BytesIO(decoded)
                data = io.BytesIO()
                pd.read_excel(excel_data).to_csv(data, index=False)
                data.seek(0)
            else:
                data = io.BytesIO(decoded)
                
            blob_client.upload_blob(data.read(),overwrite=True)
            # Upload file to Azure Blob Storage
            # blob_client.write(blob_name, decoded)

            return html.Div([
                html.H5(
                    f"File uploaded successfully to container '{container_name}' with blob name '{blob_name}'."),
                html.Hr(),
                html.P(f"Original File Name: {filename}")
            ])
        except Exception as e:
            return html.Div([html.H5(f"Error: {e}")])

    return html.Div("No file uploaded yet.")


# Callback to handle API trigger
@app.callback(
    Output('output-api-trigger', 'children'),
    Input('api-trigger-button', 'n_clicks'),
    State('api-url', 'value')
)
def trigger_api(n_clicks, api_url):
    if n_clicks == 0:
        return ""

    if not api_url:
        return html.Div("Please enter a valid API URL.")

    try:
        response = requests.get(api_url,verify=False)
        if response.status_code == 200:
            return html.Div([
                html.H5("API Triggered Successfully"),
                html.P(f"Response: {response.text}")
            ])
        else:
            return html.Div([
                html.H5("API Trigger Failed"),
                html.P(f"Status Code: {response.status_code}"),
                html.P(f"Response: {response.text}")
            ])
    except Exception as e:
        return html.Div([html.H5(f"Error: {e}")])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)
