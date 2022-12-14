import asyncio, time
from multiprocessing import Process, Pool
import logging
import sys, os
from async_func import aiohttp_html_to_rabbit, create_amqp_connection
from sync_func import get_custum_url, get_product_urls, make_directory_tree, consume_parse_save


# define variables ?PAGEN_1=2
BASE_PATH = './'
BASE_URL = 'https://romatti.ru/catalog'
url_product_list = []
path_product_list = []
tasks_save_pics = []
tasks_save_txts = []


async def async_main():
    # Get products urls
    for url_section in get_custum_url():
        get_product_urls(url_section, path_product_list, url_product_list, BASE_URL)
    # await asyncio.gather(*[get_product_urls(url_section, path_product_list, url_product_list, BASE_URL) for url_section in get_custum_url()])
    # Make products directories
    # func_list = [make_directory_tree(path, BASE_URL, BASE_PATH) for path in path_product_list]
    for path in path_product_list: make_directory_tree(path, BASE_URL, BASE_PATH)
    # for func in func_list: func()
    # Sending to Rabbit HTMLs
    amqp_connection = await create_amqp_connection()
    await asyncio.gather(*[aiohttp_html_to_rabbit(url_product, path_product, amqp_connection) for url_product, path_product in zip(url_product_list, path_product_list)])

    # Здеся процессы кансамят хтмлки, парсят их и сохраняют в джисон

def main():
    # start workers in background
    amqp_host = os.getenv('AMQP_HOST')
    amqp_port = os.getenv('AMQP_PORT')
    amqp_user = os.getenv('AMQP_USER')
    amqp_pass = os.getenv('AMQP_PASSWORD')
    amqp_address = f'amqp://{amqp_user}:{amqp_pass}@{amqp_host}:{amqp_port}'

    workers = []
    num_workers = 4
    for i in range(num_workers):
        worker = Process(target=consume_parse_save, args=[amqp_address])
        workers.append(worker)
        worker.start()

    # start async loop
    asyncio.run(async_main())



if __name__ == '__main__':
    start_time = time.time()
    """Algorithm.
    Logger
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
    logging.basicConfig(level=logging.INFO, handlers=[console])
    main()
    execution_time = round(time.time() - start_time, 3)
    print(f'Время выполнения программы: {execution_time} с.')