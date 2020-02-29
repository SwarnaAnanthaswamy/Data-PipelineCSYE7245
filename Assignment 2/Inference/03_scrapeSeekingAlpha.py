#!/usr/bin/env python3
import scrapy
from scrapy.crawler import CrawlerProcess  # Programmatically execute scrapy
from urllib.parse import urlparse
# from slugify import slugify
import json
import re
import boto3
import pandas as pd

# RANDOMIZE USER AGENTS ON EACH REQUEST:
import random

s3 = boto3.resource('s3',
                    aws_access_key_id='',
                    aws_secret_access_key='')

s3.Bucket('assignment2swarna').download_file(
    'stage/filteredRecords.csv', 'filteredRecords.csv')

df = pd.read_csv('filteredRecords.csv')
print(df)
ticker = df['Ticker'][0]
year = '-' + str(df['Year'][0]) + '-'
# SRC: https://developers.whatismybrowser.com/useragents/explore/
user_agent_list = [
    # Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    # Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]
debug_mode = False

with open("proxyList.txt") as file_in:
    proxylist = []
    for line in file_in:
        proxylist.append(line)


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        # 'LOG_LEVEL': 'CRITICAL', # 'DEBUG'
        'LOG_ENABLED': True,
        'DOWNLOAD_DELAY': 1  # 0.25 == 250 ms of delay, 1 == 1000ms of delay, etc.
    }

    def start_requests(self):
        # GET LAST INDEX PAGE NUMBER
        urls = ['https://seekingalpha.com/earnings/earnings-call-transcripts/9999']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_last_page)

    def parse_last_page(self, response):
        data = response.css("#paging > ul.list-inline > li:last-child a::text")
        last_page = data.extract()
        last_page = int(last_page[0])
        user_agent = random.choice(user_agent_list)
        # for x in range(0, 1):
        #     # DEBUGGING: CHECK ONLY FIRST ELEMENT
        #     if debug_mode == True and x > 0:
        #         break
        #     url = "https://seekingalpha.com/earnings/earnings-call-transcripts/%d" % (x)
        #     yield scrapy.Request(url=url, callback=self.parse, headers={'User-Agent': user_agent})

        url = "https://seekingalpha.com/symbol/%s/earnings/transcripts" % ticker
        user_agent = random.choice(user_agent_list)
        yield scrapy.Request(url=url, callback=self.parse, headers={'User-Agent': user_agent})

    # SAVE CONTENTS TO AN HTML FILE
    def save_contents(self, response):
        data = response.css("div#content-rail article #a-body")
        data = data.extract()

        url = urlparse(response.url)
        url = url.path
        print(url)

        # if (str(url).find(year)) >= 0:
        print(str(url).find(year))

        filename = str(url) + ".html"
        a = filename.replace('/', '-')
        with open(a, 'w') as f:
            f.write(data[0])
            f.close()

        filename2 = 'result.txt'
        with open(filename2, 'a') as f:
            # Remove HTML tags
            clean = re.compile('<.*?>')
            res = re.sub(clean, '', data[0])
            f.write(res)
            f.close()

        s3 = boto3.resource('s3',
                            aws_access_key_id='',
                            aws_secret_access_key='')

        s3.Bucket('assignment2swarna').upload_file(
            'result.txt', 'stage/result.txt')

        print('File Uploaded')

        # Remove blank spaces
        # res = res.replace(' ', '')

        # Write pre-processed data into the text file
        # print(res)

    def parse(self, response):

        print("Parsing results for: " + response.url)
        links = response.css("a[sasource='qp_analysis']")
        links.extract()
        for index, link in enumerate(links):
            url = link.xpath('@href').extract()
            # DEBUGGING MODE: Parse only first link
            if debug_mode == True and index > 0:
                break
            url = link.xpath('@href').extract()
            data = urlparse(response.url)
            data = data.scheme + "://" + data.netloc + url[0]  # .scheme, .path, .params, .query
            user_agent = random.choice(user_agent_list)
            print("======------======")
            print("Getting Page:")
            print("URL: " + data)
            print("USER AGENT: " + user_agent)
            print("======------======")

            proxy = 'http://' + str(random.choice(proxylist)[:-2])
            print(proxy)

            request = scrapy.Request(data, callback=self.save_contents, headers={'User-Agent': user_agent},
                                     meta={'proxy': 'http://118.175.93.148:42409'})
            yield request


c = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0',
})
c.crawl(QuotesSpider)
c.start()
