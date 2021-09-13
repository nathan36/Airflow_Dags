from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import random

class parser():
    def __init__(self, area, type, max_price, min_year_built):
        self.area = area
        self.type = type
        self.max_price = max_price
        self.min_year_built = min_year_built
        self.data = None

    def get_request(self):
        headers = ({'User-Agent':
                        'Mozilla/5.0 (Windows NT 6.1) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/41.0.2228.0 Safari/537.36'})
        data = []
        stop_flag = False
        page = 1

        while stop_flag == False:
            rew_url = "https://www.rew.ca/properties/" \
                      "areas/{}/" \
                      "type/{}".format(self.area,
                                       self.type)
            page_url = "/page/" + str(page)
            filter_url = "?list_price_to={}" \
                         "&year_built_from={}".format(self.max_price,
                                                      self.min_year_built)
            if page == 1:
                url = rew_url + filter_url
            else:
                url = rew_url + page_url + filter_url

            response = requests.get(url, headers=headers)
            if response.status_code == requests.codes.ok:
                data.append(response.text)
                print('finish parsing content from page {}'.format(page))
                page += 1
            else:
                stop_flag = True
            time.sleep(max(random.gauss(3, 1), 2))
        self.data = data

    def parse_data(self):
        data = []
        for item in self.data:
            page_html = BeautifulSoup(item, "html.parser")
            house_containers = page_html.find_all("div", class_="displaypanel-body")
            row = {}
            if house_containers != []:
                for container in house_containers:
                    # price
                    price_container = container.find_all("div", class_="displaypanel-title hidden-xs")
                    if price_container != []:
                        price = price_container[0].text
                        price = int(price.replace("\n", "").replace("$", "").replace(",", ""))
                    else:
                        price = None
                    # location
                    location_container = container.find_all("a")
                    if location_container != []:
                        location = location_container[0].get("href").split("?")[0].split("/")[-1]
                    else:
                        location = None
                    row['location'] = location
                    # size
                    size_container = container.find_all("ul", class_="l-pipedlist")
                    if len(size_container) > 1:
                        size = int(size_container[1].find_all("li")[-1].text.split()[0])
                    else:
                        size = None
                    if price and location and size:
                        row['price'] = price
                        row['location'] = location
                        row['size'] = size
            if bool(row):
                data.append(row)
        return data