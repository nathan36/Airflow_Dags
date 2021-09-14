from airflow.models import DagBag
import unittest

class Test_LoadPropertyData(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(include_examples=False)

    def test_dag_loaded(self):
        self.dagbag.process_file('load_property_data.py')
        assert self.dagbag.import_errors == {}
        assert self.dagbag is not None
