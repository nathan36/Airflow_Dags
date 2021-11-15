from bs4 import BeautifulSoup
from requests import get
from dataclasses import dataclass, field
from typing import List, Dict
import pandas as pd
from datetime import date

@dataclass
class Page:
    containers: List
    empty: bool = True


@dataclass
class Scrper:
    headers: Dict
    filter: Dict
    prices: List = field(default_factory=list)
    locations: List = field(default_factory=list)
    lot_sizes: List = field(default_factory=list)
    stop_flag: bool = False
    page: int = 1

    def create_url(self) -> tuple:
        base_url = "https://www.rew.ca/properties/" \
                          "areas/{}/" \
                          "type/{}".format(self.filter['area'],
                                           self.filter['type'])
        filter_url = "?list_price_to={}" \
                     "&year_built_from={}".format(self.filter['max_price'],
                                                  self.filter['min_year_built'])
        return base_url, filter_url

    def parse_html(self) -> List[Page]:
        pages = []
        base_url, filter_url = self.create_url()
        while self.stop_flag == False:
            if self.page == 1:
                full_url = base_url + filter_url
            else:
                full_url = base_url + "/page/" + str(self.page) + filter_url
            response = get(full_url, headers=self.headers)
            page_html = BeautifulSoup(response.text, 'html.parser')
            house_containers = page_html.find_all("div", class_="displaypanel-body")
            if house_containers and len(house_containers) != 0:
                pages.append(Page(containers=house_containers))
                self.page += 1
            else:
                self.stop_flag = True
            print('finish parsing content from page {}'.format(self.page - 1))
        return pages

    def get_price(self, container) -> int:
            price_container = container.find_all("div", class_="displaypanel-title hidden-xs")
            if price_container and len(price_container) != 0:
                price = price_container[0].text
                price = int(price.replace("\n", "").replace("$", "").replace(",", ""))
            else:
                price = 0
            return price

    def get_location(self, container) -> str:
            location_container = container.find_all("a")
            if location_container and len(location_container) != 0:
                location = location_container[0].get("href").split("?")[0].split("/")[-1]
            else:
                location = "None"
            return location

    def get_lot_size(self, container) -> int:
            size_container = container.find_all("ul", class_="l-pipedlist")
            if size_container and len(size_container) > 1:
                size = int(size_container[1].find_all("li")[-1].text.split()[0])
            else:
                size = 0
            return size

    def _get_content(self):
        pages: List[Page] = self.parse_html()
        for page in pages:
            self.prices = list(map(lambda container: self.get_price(container), page.containers))
            self.locations = list(map(lambda container: self.get_location(container), page.containers))
            self.lot_sizes = list(map(lambda container: self.get_lot_size(container), page.containers))

    def get_content(self) -> pd.DataFrame:
        self._get_content()
        return pd.DataFrame({'Price': self.prices,
                             'Location': self.locations,
                             'Size': self.lot_sizes,
                             'ParsedDt': date.today().strftime('%Y-%m-%d %H:%M:%S')})





