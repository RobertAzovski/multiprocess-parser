import aiohttp, aio_pika
import os
from dotenv import load_dotenv

load_dotenv()


async def create_amqp_connection():
    # Rabbit connection
    amqp_host = os.environ.get('AMQP_HOST')
    amqp_port = os.environ.get('AMQP_PORT')
    amqp_user = os.environ.get('AMQP_USER')
    amqp_password = os.environ.get('AMQP_PASSWORD')
    # throw error if amqp details not defined
    if not all([amqp_host, amqp_port, amqp_user, amqp_password]):
        raise NotImplementedError
    
    amqp_connection = await aio_pika.connect_robust(
                                host=amqp_host,
                                port=amqp_port,
                                login=amqp_user,
                                password=amqp_password)

    queue_name = "html_data"
    async with amqp_connection.channel() as ch:
        await ch.declare_queue(queue_name, auto_delete=False)

    return amqp_connection


async def fetch(client, url_product):
    async with client.get(url_product) as resp:
        assert resp.status == 200
        return await resp.text()

async def aiohttp_html_to_rabbit(url_product, path_product, amqp_connection):
    """Creating aiohttp session, calls fetch html func and calls pika produce"""
    async with aiohttp.ClientSession() as client:
        message = f'{path_product}:{await fetch(client, url_product)}'
        await aio_pika_produce_message(message, amqp_connection)


# AIO_PIKA produce code
async def aio_pika_produce_message(html, amqp_connection):
    """Receiving html and sending to a queue"""
    async with amqp_connection.channel() as channel:
        await channel.default_exchange.publish(
            aio_pika.Message(body=html.encode()),
            routing_key='html_data',
        )