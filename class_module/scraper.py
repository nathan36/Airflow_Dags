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
class Scraper:
    headers: Dict
    filter: Dict
    data: List[tuple] = field(default_factory=list)
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

    def get_info(self, container) -> tuple:
        price_container = container.find_all("div", class_="displaypanel-title hidden-xs")
        location_container = container.find_all("a")
        size_container = container.find_all("ul", class_="l-pipedlist")
        if price_container or location_container \
                or (size_container and len(size_container) > 1):
            price_txt = price_container[0].text
            price = int(price_txt.replace("\n", "").replace("$", "").replace(",", ""))
            location = location_container[0].get("href").split("?")[0].split("/")[-1]
            label_lst = size_container[1].find_all("li")
            size = int([ele.text.split()[0] for ele in label_lst if 'sf' in ele.text][0])
            return (price, location, size)

    def _get_content(self) -> None:
        pages: List[Page] = self.parse_html()
        for page in pages:
            self.data = list(map(lambda container: self.get_info(container), page.containers))

    def get_content(self) -> list:
        self._get_content()
        return [tup for tup in self.data if tup]





