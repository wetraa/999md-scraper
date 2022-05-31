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

from middleware import last_fetch, log_fetch, limit, retry
from utils import arun, amap, write_json


BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
}

ROOT_URL = 'https://999.md'
MOBILE_ROOT_URL = 'https://m.999.md'
OUTPUT_FILE_PATH = '../999md_parsing_result.csv'


FETCH_LIMIT = 1


scraped_products_urls = set()

users = {}


def main():
    arun(scrape_products())


async def scrape_products():
    await write_to_csv(
        data=(
            'Название',
            'Ссылка',
            'Цена',
            'Телефоны',
            'Username',
            'Тип',
            'Локация',
            'Описание'
        ),
        flag='w'
    )
    # await scrape_product('https://m.999.md/ru/75587315')
    # await scrape_user('https://m.999.md/ru/profile/www-fluture-md')
    # return
    page = await fetch(ROOT_URL + '/ru/')
    main_categories_urls = page.css('.main-CatalogNavigation a[data-category]').attrs('href').map(page.abs)
    for category_url in main_categories_urls:
        await scrape_products_by_category(category_url)


async def scrape_products_by_category(category_url):
    page = await fetch(category_url)
    subcategories_urls = list(set(
        url.split('?')[0]
        for url in page.css('.category__subCategories-collection a[data-category]').attrs('href').map(page.abs)
    ))
    for url in subcategories_urls:
        await scrape_products_by_subcategory(url)


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

    await parse_page_products(page)
    for url in page_urls:
        await scrape_page_products(url)


async def scrape_page_products(category_url):
    page = await fetch(category_url)
    await parse_page_products(page)


async def parse_page_products(page):
    urls = [MOBILE_ROOT_URL + href for href in page.css('.ads-list-photo-item-animated-link').attrs('href')]
    for url in urls:
        await scrape_product(url)


async def scrape_product(product_url, product_data=None):
    global scraped_products_urls, users
    if product_url in scraped_products_urls:
        return
    scraped_products_urls.add(product_url)
    
    if product_data is None:
        product_data = {}

    page = await fetch(product_url)
    product_data.update(page.root.multi({
        'url': C.const(page.url),
        'title': C.css('.item-page__meta__title').first.text.strip(),
        'location': C.css('.item-page__author-info_marker span').first.text.strip(),
        'user_type': C.css('.item-page__meta--header__type').first.text,
        'description': C.css('.item-page__info__text').first.inner_text.strip()
    }))

    for div in page.css('.item-page__meta__price-feature__prices__price'):
        if div.cssselect('[content=EUR]'):
            try:
                product_data['price'] = ''.join(div.cssselect('[itemprop=price]')[0].text.replace('≈', '').split())
            except Exception:
                pass
    el = page.css('.item-page__author-info__item_user').first
    product_data['username'] = el.text.strip()
    user_url = page.abs(el.get('href'))
    if user_url not in users:
        users[user_url] = await scrape_user(user_url)
    product_data.update(users[user_url])

    await save_product(product_data)


async def scrape_user(user_url):
    page = await fetch(user_url)
    phones = []
    for el in page.css('.user-profile__tab-contacts__phone'):
        phones.append(el.get('href').replace('tel:', ''))

    user_data = {
        'phones': '\n'.join(phones),
    }
    return user_data


async def save_product(data):
    if not data.get('phones'):
        return

    await write_to_csv((
        data['title'],
        data['url'],
        data.get('price'),
        data['phones'],
        data['username'],
        data['user_type'],
        data['location'],
        data.get('description')
    ))


async def write_to_csv(data, flag='a'):
    async with aiofiles.open(OUTPUT_FILE_PATH, flag, encoding='utf-8', newline='') as f:
        writer = AsyncWriter(f, delimiter=';', dialect='excel', quoting=csv.QUOTE_MINIMAL)
        await writer.writerow(data)


@limit(per_domain=FETCH_LIMIT)
@retry()
@log_fetch
# @last_fetch
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
