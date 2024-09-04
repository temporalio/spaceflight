from temporalio.client import Client
from temporalio.service import TLSConfig


async def get_client() -> Client:
    with open("temporal-certs/client.pem", "rb") as f:
        client_cert = f.read()
    with open("temporal-certs/client.key", "rb") as f:
        client_key = f.read()
    host = "temporalinspace.a2dd6.tmprl.cloud:7233"
    print(f"Attempting to connect to Temporal @ {host}")
    client = await Client.connect(
        host,
        tls=TLSConfig(
            client_cert=client_cert,
            client_private_key=client_key,
        ),
        namespace="temporalinspace.a2dd6",
    )
    print("Connected to Temporal")
    return client
