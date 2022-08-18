import unittest
from unittest import TestCase
import pandas as pd
from modules.tasks import transform_revenue_fn

class TestTasks(TestCase):

    def test_transform_revenue(self):
        df = pd.DataFrame({"price": [1, 2, 3]})
        transformed_json = transform_revenue_fn(df.to_json())
        dft = pd.read_json(transformed_json)        
        self.assertTrue(dft["price_in_cents"].values ==  [100,200,300])

if __name__ == '__main__':
    unittest.main()