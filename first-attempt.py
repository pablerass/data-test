#!/usr/bin/env python3
"""Random work on data set calculations."""
import pandas as pd
import os
import requests

from urllib.parse import urlparse
from contextlib import contextmanager
from collections import defaultdict
from time import time
from operator import itemgetter


@contextmanager
def timing(description: str) -> None:
    """Get execution times."""
    start = time()
    yield
    ellapsed_time = time() - start

    print(f"{description}: {ellapsed_time}")


data_url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz"
_, _, data_file = urlparse(data_url).path.rpartition('/')

if not os.path.exists(data_file):
    r = requests.get(data_url, allow_redirects=True)
    r.raise_for_status()
    open(data_file, 'wb').write(r.content)

df = pd.read_csv(data_file, compression='gzip', header=0, sep='\t',
                 converters={"verified_purchase": lambda v: v == 'Y',
                             "vine": lambda v: v == 'Y'})
data = df.to_dict(orient='records')

# Mean
with timing("Normal rating mean"):
    ratings = [d['star_rating'] for d in data]
    print(sum(ratings) / len(ratings))
with timing("Pandas rating mean"):
    print(df['star_rating'].mean())

# Distribution
with timing("Normal rating distribution"):
    distribution_dict = defaultdict(int)
    for d in data:
        distribution_dict[d['star_rating']] += 1
    print(dict(distribution_dict))
with timing("Pandas rating distribution"):
    print(df.groupby('star_rating').size().to_dict())

# Verified vs non-verified
with timing("Normal verfied vs non verified"):
    verified_purchase_dict = defaultdict(int)
    for d in data:
        verified_purchase_dict[d['verified_purchase']] += 1
    print(dict(verified_purchase_dict))
with timing("Pandas verified vs non verified"):
    print(df.groupby('verified_purchase').size().to_dict())

# Product reviews count
with timing("Normal product reviews"):
    product_dict = defaultdict(int)
    for d in data:
        product_dict[d['product_id']] += 1
    sorted_products = sorted(product_dict.items(), key=itemgetter(1), reverse=True)
    print(dict(sorted_products[:10]))
with timing("Pandas product reviews"):
    print(df.groupby('product_id').size().nlargest(10).to_dict())

# Product rating
with timing("Normal product reviews mean"):
    product_ratings_list = defaultdict(list)
    for d in data:
        product_ratings_list[d['product_id']].append(d['star_rating'])
    product_ratings_mean = {}
    for p, ratings in product_ratings_list.items():
        if len(ratings) > 50:
            product_ratings_mean[p] = sum(ratings) / len(ratings)
    sorted_product_ratings = sorted(product_ratings_mean.items(), key=itemgetter(1), reverse=True)
    print(dict(sorted_product_ratings[:10]))
with timing("Pandas product reviews mean"):
    # Solution 1
    # print(df.groupby('product_id').filter(lambda x: len(x) > 50)['star_rating'].mean().nlargest(10).to_dict())

    # Solution 2
    # print(df.groupby('product_id').agg(mean=('star_rating', 'mean'), count=('star_rating', 'count')).
    #       groupby('product_id').filter(lambda x: x['count'] > 50)['mean'].nlargest(10).to_dict())

    # Solution 3
    # dff = df['product_id'].value_counts()[lambda x: x>50]
    # dfm = df.groupby('product_id')['star_rating'].mean().to_frame()
    # print(dfm.join(dff, how='inner')['star_rating'].nlargest(10).to_dict())

    # Solution 4
    print(df.groupby('product_id').
          agg(mean=('star_rating', 'mean'), count=('star_rating', 'count'))[lambda x: x['count'] > 50]['mean'].
          nlargest(10).to_dict())
