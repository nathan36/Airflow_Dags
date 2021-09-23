from airflow.models import DagBag
import unittest
from class_module import property_parser as pp

class Test_LoadPropertyData(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(include_examples=False)
        self.area = ["metrotown-bc","burnaby-bc","brentwood-park-bc"]
        self.type = 'apartment-condo'
        self.max_price = 650000
        self.min_year_built = 2015

    def test_check_area(self):
        session = pp.parser(self.area, self.type,
                            self.max_price, self.min_year_built)
        session.check_area()
        area_list_with_error = session.area_list_with_error
        status = session.response_status
        self.assertEqual(area_list_with_error, ["metrotown-bc","brentwood-park-bc"])
        self.assertEqual(status, "error")

    def test_dag_loaded(self):
        self.dagbag.process_file('load_property_data.py')
        self.assertFalse(self.dagbag.import_errors)
        self.assertIsNotNone(self.dagbag)



