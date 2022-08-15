from random import random
import pandas as pd, random
from faker import Faker


fake = Faker()
dates = pd.date_range(start="2022-08-01",end="2022-08-10")

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
    df.to_csv(f"inbound/revenue-{d}.csv")

for d in dates:
    d = d.strftime("%Y-%m-%d")
    data = []
    for i in range(random.randint(10,1000)):
        profile = fake.profile()
        data.append(profile)
    df = pd.DataFrame(data)
    df.to_csv(f"inbound/user-{d}.csv")