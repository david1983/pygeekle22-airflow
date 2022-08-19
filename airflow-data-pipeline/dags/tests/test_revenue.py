import pandas as pd
from unittest import TestCase
from modules.tasks import transform_revenue_fn


class TestRevenue(TestCase):
# Test the transform_revenue_fn function
    def test_transform_revenue_fn(self):
        df = pd.DataFrame({"price": [1, 2, 3]})
        df_json = transform_revenue_fn(df.to_json())  
        dft = pd.read_json(df_json)           
        expected = [100, 200, 300]
        self.assertTrue(all(dft["price_cents"].values == expected))