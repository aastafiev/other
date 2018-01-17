#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import json
import aiopg.sa
# import sqlalchemy as sa

from scipy.interpolate import interp1d
import numpy as np

import os
import settings as st

from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# import odometer.db as db


OUT_FILE = os.path.join(st.PROJECT_DIR, 'odometer', 'data', 'train.json')

LAG = -5


def interpolate(x, y, xnew):
    f = interp1d(x, y, kind='linear', fill_value='extrapolate')
    ynew = f(xnew)
    ynew[ynew < 0] = 0
    return ynew


async def prepare_train_test(get_conn, set_conn):
    query = '''
        select 
            *
        from client_time_series_train tr
        where tr.odometer >= 5000
        order by
            tr.client_name,
            tr.vin,
            tr.yyy,
            tr.mmm;
    '''

    group_values = []
    train_values = []
    prev_key = None
    for row in tqdm(await get_conn.execute(query)):
        current_key = '|'.join([row.client_name, row.vin])

        if prev_key and group_values and current_key != prev_key and len(group_values) > 1:
            # train_values.append(group_values[-1])
            x, y = tuple(), tuple()
            for group_values_line in group_values:
                x += (group_values_line['mmm'], )
                y += (group_values_line['odometer'], )

            x_new = tuple(i for i in range(1, x[-1]+1))
            y_new = interpolate(x, y, x_new)
            group_values[-1]['mean_km'] = np.mean(y_new[LAG:])
            train_values.append(group_values[-1])
            group_values = []

        group_values.append({'region': row.region,
                             'bir': row.bir,
                             'client_name': row.client_name,
                             'vin': row.vin,
                             'model': row.model,
                             'yyy': row.yyy,
                             'mmm': row.mmm,
                             'odometer': row.odometer})
        prev_key = current_key

        # rows_counter += 1

        with open(OUT_FILE, 'w') as fout:
            json.dump(train_values, fout)


async def get_postgres_engine(loop, db):
    engine = await aiopg.sa.create_engine(**db, loop=loop)
    return engine


async def main(loop, db_config):
    pg_get = await get_postgres_engine(loop, db_config)
    pg_set = await get_postgres_engine(loop, db_config)
    async with pg_get.acquire() as conn_get, pg_set.acquire() as conn_set:
        await prepare_train_test(conn_get, conn_set)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    db_conf = {'database': 'test',
               'host': 'localhost',
               'user': 'postgres',
               'password': '123',
               'port': '5432'}

    loop.run_until_complete(main(loop, db_conf))
