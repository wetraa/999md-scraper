import csv
import json
import os
import re
import math
import string
import time
from pprint import pprint

import aiohttp
import aiofiles
from aiocsv import AsyncWriter
import parsechain
from parsechain import C

from middleware import log_fetch, limit, retry
from utils import arun, amap, write_json


BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
}

ROOT_URL = 'https://999.md'
MOBILE_ROOT_URL = 'https://m.999.md'
OUTPUT_FILE_PATH = '../999md_parsing_result.csv'


FETCH_LIMIT = 7


def main():
    arun(scrape_products())


async def scrape_products():
    await write_to_csv(
        data=(
            'Название',
            'Ссылка',
            'Цена',
            'Телефон',
            'Имя'
        ),
        flag='w'
    )
    page = await fetch(ROOT_URL + '/ru/')
    main_categories_urls = page.css('.main-CatalogNavigation a[data-category]').attrs('href').map(page.abs)
    await amap(scrape_products_by_category, main_categories_urls)


async def scrape_products_by_category(category_url):
    page = await fetch(category_url)
    subcategories_urls = list(set(
        url.split('?')[0]
        for url in page.css('.category__subCategories-collection a[data-category]').attrs('href').map(page.abs)
    ))
    await amap(scrape_products_by_subcategory, subcategories_urls)


async def scrape_products_by_subcategory(category_url):
    page = await fetch(category_url)
    region_filter_id = None
    for div in page.css('[data-filter-id]'):
        if div.cssselect('.items__filters__filter__label__title')[0].text.strip() in ('Регион', 'Pегион'):
            region_filter_id = div.get('data-filter-id')
            break
    else:
        if category_url in ['https://999.md/ru/list/real-estate/real-estate-abroad']:
            return
        raise Exception(f'Изменения в дизайне. Проблемы с выбором региона [{category_url}]')

    category_url = f'{category_url}?o_{region_filter_id}_7=12900,12886'
    page = await fetch(category_url)
    total_ads = int(page.css('#js-total-ads').text[1:-1])
    total_pages = math.ceil(total_ads / 84)
    page_urls = [
        f'{category_url}&page={page}'
        for page in range(2, total_pages + 1)
    ]
    pprint(page_urls)

    # await scrape_page_products(category_url)
    # await amap(scrape_page_products, page_urls)


async def scrape_page_products(category_url):
    page = await fetch(category_url)
    urls = [MOBILE_ROOT_URL + href for href in page.css('.ads-list-photo-item-animated-link').attrs('href')]
    await amap(scrape_product, urls)


async def scrape_product(product_url):
    page = await fetch(product_url)
    data = page.root.multi({
        'url': C.const(product_url),
        'title': C.css('.item-page__meta__title').first.text.strip(),
        'price': C.css('.item-page__meta__price-feature__prices__price__value').first.text.strip(),
        'phone': C.css('.item-page__author-info__item_phone').map(lambda el: el.text).first,
        'username': C.css('.item-page__author-info__item_user').first.text.strip(),
        'location': C.css('.item-page__author-info_marker span').first.text.strip()
    })
    await save_product(data)


async def save_product(data):
    await write_to_csv((
        data['title'],
        data['url'],
        data['price'],
        data.get('phone') or '',
        data.get('username') or ''
    ))


async def write_to_csv(data, flag='a'):
    async with aiofiles.open(OUTPUT_FILE_PATH, flag) as f:
        writer = AsyncWriter(f)
        await writer.writerow(data)



def get_categories_urls():
    soup = get_soup(ROOT_URL + '/ru/')
    categories = [
        {'name': a.text, 'url': ROOT_URL + a.get('href')}
        for a in soup.find(class_='main-CatalogNavigation').findAll('a', {'data-category': True})
    ]
    links = []
    for cat in categories:
        soup = get_soup(cat['url'])
        for div in soup.findAll(class_='category__subCategories-collection'):
            links += div.findAll('a', {'data-subcategory': True}, recursive=False)

    return links


# def write_to_csv(data, flag='a'):
#     # with open(OUTPUT_FILE_PATH, flag, newline='', encoding='utf-8-sig') as f:
#     #     writer = csv.writer(f, delimiter=';', dialect='excel', quoting=csv.QUOTE_MINIMAL)
#     #     writer.writerow(data)
#     pass


@limit(per_domain=FETCH_LIMIT)
@retry()
@log_fetch
async def fetch(url, method='get', data=None, cookies=None, proxy=None):
    async with aiohttp.ClientSession(cookies=cookies, timeout=aiohttp.ClientTimeout(total=60), headers=BASE_HEADERS) as s:
        async with getattr(s, method)(url, data=data, proxy=proxy) as resp:
            body = await resp.text()
            return parsechain.Response(
                method=resp.method, url=str(resp.url), body=body,
                status=resp.status, reason=resp.reason,
                headers=dict(resp.headers)
            )


if __name__ == '__main__':
    main()
