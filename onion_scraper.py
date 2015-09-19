from bs4 import BeautifulSoup
from pymongo import MongoClient
import requests
import datetime
import random as rnd
import time
import re


def init_mongo():
    client = MongoClient()
    db = client.onion
    collection = db.articles
    return db, collection


def parse_date(pubdate):
    date = ''
    if 'Sept' in pubdate:
        pubdate = pubdate.replace('Sept', 'Sep')

    try:
        date = datetime.datetime.strptime(pubdate, '%B %d, %Y')
    except ValueError:
        pass

    try:
        date = datetime.datetime.strptime(pubdate, '%b. %d, %Y')
    except ValueError:
        pass

    if date == '':
        print 'unable to parse:', pubdate
    return date


def write_summary_to_mongo(soup):
    article_summary = soup.findAll('article', class_='summary')

    for tag in article_summary[1:]:
        aTag = tag.find('a', class_='handler')

        href = aTag['href']
        art_id = int(href.split("-")[-1])

        if db.articles.find_one({'article_id': art_id}):
            print "duplicate article:", art_id
            continue

        if 'article/' in href:
            date = parse_date(str(aTag['data-pubdate']))

            if date != '':
                print href
                meta_data = {}
                meta_data['article_id'] = art_id
                meta_data['href'] = href
                meta_data['headline'] = tag.find('h2').find('a')['title']
                meta_data['pub-date'] = date.strftime('%Y%m%d')
                meta_data['content-type'] = \
                    str(tag.find('span', class_='content-feature-type')
                        .text.strip())

                collection.insert_one(meta_data)


def scraper_meta_data(npages):
    url = 'http://www.theonion.com/search?page='

    for i in range(npages):
        offset = 1177
        this_url = url + str(i+offset)
        html = requests.get(this_url)
        if html.status_code == 200:
            print this_url
            soup = BeautifulSoup(html.content, "html.parser")
            write_summary_to_mongo(soup)
        else:
            print 'broken: ', url + (i+offset)
        time.sleep(rnd.uniform(3, 7))


def scrape_articles():
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
              "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
              "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

    states = [state.lower() for state in states]

    bad_id = []
    with open('ignore_articles.txt') as f:
        for row in f:
            bad_id.append(int(row))

    for article in collection.find({'href': {'$exists': True},
                                    'city': {'$exists': False}}):
        if 'NEWS' not in article['content-type']:
            continue
        if article['article_id'] in bad_id:
            continue

        url = 'http://www.theonion.com' + article['href']
        html = requests.get(url)

        if html.status_code == 200:
            soup = BeautifulSoup(html.content, "html.parser")
            txt = soup.find('div', class_='content-text').text.strip()

            print url
            alltxt = []
            if u'\u2014' in txt:
                alltxt = txt.split(u'\u2014')
            if u'\x97' in txt:
                alltxt = txt.split(u'\x97')
            # if u'-' in txt:
            #     alltxt = txt.split(u'-')

            if len(alltxt) == 0:
                print "invalid format, cannot find city, state",\
                    article['article_id']
                continue

            city = ''
            state = ''
            if ',' in alltxt[0]:
                words = alltxt[0].lower().split(',')
                if len(words) == 2:
                    city, state = words
            else:
                city = alltxt[0].lower()

            txt = re.sub('[^\w\s]+', ' ', alltxt[1]).lower()
            if len(city) > 0:
                collection.update({'_id': article['_id']},
                                  {'$set': {'city': city,
                                            'state': state, 'content': txt}})
                # print city, state, '\n', txt

        time.sleep(rnd.uniform(1, 3))

if __name__ == '__main__':
    db, collection = init_mongo()
    # scraper_meta_date(15)
    scrape_articles()
