import json
from bs4 import BeautifulSoup
import winshell, os, logging
import time
import pika
from pathlib import Path
from urllib.request import urlopen
from dataclasses import dataclass


class Product():
    def __init__(self) -> None:
        self.artikul = None
        self.name = None
        self.price = None
        self.photos_path = None
        self.description = None
        self.ops = None
        self.variations = None

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)


def make_directory_tree(url_product, BASE_URL, BASE_PATH):
    Path(create_path(f'{url_product}/f/', BASE_URL, BASE_PATH)).mkdir(parents=True, exist_ok=True)


def save_json(path_product, product):
    with open(create_path(f'{path_product}/{product.name}.json'), 'w', encoding='utf-8') as file:
                file.write(product.toJSON())


def get_custum_url():
    with open('urls.txt', 'r') as file:
        for url_section in file:
            yield url_section.rstrip()


def create_path(url_product, BASE_URL, BASE_PATH):
    url_parts = url_product.replace(BASE_URL, '')
    new_path = os.path.abspath(os.sep.join([BASE_PATH, url_parts]))
    return new_path


def get_html(url_section):
    with urlopen(url_section) as response:
        html = response.read()
        return html


def get_product_urls(url_section, path_product_list, url_product_list, BASE_URL):
    soup = BeautifulSoup(get_html(url_section), 'lxml')
    i = 1
    if (soup.find('li', {'class': 'bx-active'}).find('span').string == f'{i}'):
        print('True pagination')
        isHaveNextPage = True
        i = 2
    while isHaveNextPage:
        soup = BeautifulSoup(get_html(f'{url_section}?PAGEN_1={i}'), 'lxml')
        for link in soup.findAll('a', {'class': 'picture_wrapper'}):
            try:
                whole_product_path = url_section.replace(BASE_URL, '') + link['href'].replace('/catalog', '').replace('.html', '/')
                path_product_list.append(whole_product_path)
                url_product_list.append(BASE_URL + link['href'].replace('/catalog', ''))
            except KeyError:
                pass
        print('One more pagi page')
        i += 1
        soup = BeautifulSoup(get_html(f'{url_section}?PAGEN_1={i}'), 'lxml')
        if (soup.find('li', {'class': 'bx-active'}).find('span').string == f'{i}') is False:
            print('False pagination')
            isHaveNextPage = False
    for link in soup.findAll('a', {'class': 'picture_wrapper'}):
        try:
            whole_product_path = url_section.replace(BASE_URL, '') + link['href'].replace('/catalog', '').replace('.html', '/')
            path_product_list.append(whole_product_path)
            url_product_list.append(BASE_URL + link['href'].replace('/catalog', ''))
        except KeyError:
            pass


# PIKA consume code
def consume_parse_save(amqp_address):
    connection = pika.BlockingConnection(pika.URLParameters(amqp_address))
    channel = connection.channel()

    channel.queue_declare(queue='html_data')

    def callback(ch, method, properties, body):
        message = body.decode()
        logging.info('Starting parsing from HTML')
        get_product_page_data(message)

    channel.basic_consume(queue='html_data', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def get_product_page_data(message):
    path_product = message.split(':')[0]
    html = message.split(':')[1]
    product = Product()
    nbsp = u'\xa0'
    logging.debug('Consuming html from rabbit')
    soup = BeautifulSoup(html, 'lxml')
    logging.debug('Actually do a parsing')
    if soup.contents:
        try:
            product.artikul = soup.find('div', {'class': 'articul_code'}).find('span').string
            product.name = soup.find('div', {'class': 'articul_code'}).parent.find('h1').text.split(',')[0].replace('*', 'x')
            product.price = soup.find('div', {'class': 'product-item-detail-price-current'}).contents[0].replace(nbsp, '')
            product.photos_path = create_path(f'{path_product}/f/')
            if soup.find('div', {'data-value': 'description'}) is not None and soup.find('div', {'data-value': 'description'}).find('p'):
                product.description = soup.find('div', {'data-value': 'description'}).find('p').string
            else:
                product.description = 'Описание отсутствует'
            product.ops = dict(zip([prop.string for prop in soup.find_all('div', {'class': 'prop_title'})], [prop.string for prop in soup.find_all('div', {'class': 'prop_val'})]))
            if soup.find('div', {'class': 'product-item-scu-container-title'}):
                title = soup.find('div', {'class': 'product-item-scu-container-title'}).string
                var_list = [spec.find('div', {'class': 'product-item-scu-item-text'}).string for spec in soup.find_all('li', {'class': 'product-item-scu-item-text-container'})]
                product.variations = f'{title} - {var_list}'
            else:
                product.variations = 'Вариации отсутствуют'
            logging.info(f'{product.name} parsed')
        finally:
            save_json(path_product, product)
            logging.info(f'JSON for - {product.name} has been saved')


