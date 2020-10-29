import logging
import azure.functions as func
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import json 

# Initialize the Cosmos client
endpoint = "https://cosmoscanguro.documents.azure.com:443/"
key = 'ynYi0GyC8EoZWZb5cXXoTUoOEGNLRYOUaV7hGhLn3SV5cKXlXK1hVqB761HsdppCdZGUM3sjL43VOTzQAVuFig=='

# <create_cosmos_client>
client = CosmosClient(endpoint, key)
# </create_cosmos_client>

# Create a database
# <create_database_if_not_exists>
database_name = 'Canguro'
database = client.create_database_if_not_exists(id=database_name)
# </create_database_if_not_exists>

# Create a container
# Using a good partition key improves the performance of database operations.
# <create_container_if_not_exists>
container_name = 'ContainerCanguro'
container = database.create_container_if_not_exists(
    id=container_name, 
    partition_key=PartitionKey(path="/Iden_Codigo"),
    offer_throughput=400
)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('atributos')
    fechaInicial = req.params.get('fechaInicial')
    fechaFinal = req.params.get('fechaFinal')

    if not name and not fechaInicial and not fechaFinal:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('atributos')
            fechaInicial = req_body.get('fechaInicial')
            fechaFinal = req_body.get('fechaFinal')

    if name and fechaInicial and fechaFinal:
        
        queryAtributos = ''
        atributos = name.split(',')
        cont = 0
        if(name != '*'):
            for atr in atributos:
                if(cont == len(atributos)-1):
                    queryAtributos += 'c.'+atr+' '
                else:
                    queryAtributos += 'c.'+atr+', '
                cont += 1
        else:
            queryAtributos = '*'
        
        if(fechaInicial == '*' and fechaFinal == '*'):
            query = f"SELECT {queryAtributos} FROM c "
        elif(fechaInicial != '*' and fechaFinal != '*'):
            query = f"SELECT {queryAtributos} FROM c WHERE c.Iden_FechaParto > '{fechaInicial}' AND c.Iden_FechaParto < '{fechaFinal}'"
        elif(fechaInicial == '*'):
            query = f"SELECT {queryAtributos} FROM c WHERE c.Iden_FechaParto < '{fechaFinal}'"
        elif(fechaFinal == '*'):
            query = f"SELECT {queryAtributos} FROM c WHERE c.Iden_FechaParto > '{fechaInicial}'"

        items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))

        jsonitems = json.dumps(items)

        request_charge = container.client_connection.last_response_headers['x-ms-request-charge']

        return jsonitems
    else:
        return func.HttpResponse(
             f"Ha existido un error de servidor, los valores de atributos, fechaInicio y fechaFinal no pueden ser nulos. \nLos valores actuales son: \natributos:'{name}' \nfechaInicio:'{fechaInicial}' \nfechaFinal:'{fechaFinal}'",
             status_code=500
        )
