from random import random
import  pandas as pd, random
from faker import Faker


fake = Faker()
dates = pd.date_range(start="2021-09-09",end="2022-02-02")

for d in dates:
    d = d.strftime("%Y-%m-%d")
    data = []
    for i in range(random.randint(10,1000)):
        data.append({
            "date": d,
            "time": fake.time(),
            "barcode": fake.ean(),
            "price": random.randrange(1, 100),
            "address": fake.address()
        })

    df = pd.DataFrame(data)
    df.to_csv(f"inbound/rv-{d}.csv")

