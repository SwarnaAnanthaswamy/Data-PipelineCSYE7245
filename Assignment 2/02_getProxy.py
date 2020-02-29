from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random
import pandas as pd
import csv

ua = UserAgent()  # From here we generate a random user agent
proxies = []  # Will contain proxies [ip, port]

df = pd.DataFrame(columns=['ip', 'port'])


def listallproxy():
    proxies_req = Request('https://www.sslproxies.org/')
    proxies_req.add_header('User-Agent', ua.random)
    proxies_doc = urlopen(proxies_req).read().decode('utf8')

    soup = BeautifulSoup(proxies_doc, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    # Save proxies in the array
    for row in proxies_table.tbody.find_all('tr'):
        proxies.append({
            'ip': row.find_all('td')[0].string,
            'port': row.find_all('td')[1].string
        })

    with open("proxyList.txt", "a") as f:
        for data in proxies:
            ip = str(data['ip']) + ':' + str(data['port'])
            print(ip)
            f.write(ip)
            f.write('\n')

    f.close()

listallproxy()