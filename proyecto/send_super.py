import aiohttp
import asyncio
import json
import os

async def send_pedido(session, pedido):
    url = 'http://localhost:5000/pedido'
    async with session.post(url, json=pedido) as response:
        return await response.text()

async def main():
    dataset_path = os.path.join('datasets', 'items_super.json')
    with open(dataset_path) as f:
        pedidos = json.load(f)

    async with aiohttp.ClientSession() as session:
        tasks = [send_pedido(session, pedido) for pedido in pedidos]
        responses = await asyncio.gather(*tasks)
        for response in responses:
            print(response)

if __name__ == '__main__':
    asyncio.run(main())
