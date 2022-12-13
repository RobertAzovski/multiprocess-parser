import asyncio
import logging
import sys
from async_func import aiohttp_html_to_rabbit, create_amqp_connection
from sync_func import get_custum_url, get_product_urls, make_directory_tree


# define variables ?PAGEN_1=2
BASE_PATH = './'
BASE_URL = 'https://romatti.ru/catalog'
url_product_list = []
path_product_list = []
tasks_save_pics = []
tasks_save_txts = []


async def main():
    # Get products urls
    await asyncio.gather(*[get_product_urls(url_section) for url_section in get_custum_url()])
    # Make products directories
    func_list = [make_directory_tree(path) for path in path_product_list]
    for func in func_list: func()
    # Sending to Rabbit HTMLs
    await asyncio.gather(*[aiohttp_html_to_rabbit(url_product) for url_product in url_product_list])

    amqp_connection = create_amqp_connection()

    # Здеся процессы кансамят хтмлки, парсят их и сохраняют в джисон



if __name__ == '__main__':
    """Algorithm.
    Creating Processes
    Async Part:
    Read product category url from file
    Create paths from product urls
    Create directories from paths
    Requesting and Saving HTMLs to Rabbit by these urls
    
    Prosesses part:
    Consuming HTMLs from Rabbit
    Parsing info from them
    Saving the info in JSON file"""
    # Logging config
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s')
    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(formatter)
    console.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG, handlers=[console])