#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import json
import aiopg.sa

from scipy.interpolate import interp1d
import numpy as np

import os
import settings as st

np.seterr(all='raise')


OUT_FILE = os.path.join(st.PROJECT_DIR, 'odometer', 'data', 'train.json')

LAG = -3


def interpolate(x, y, xnew):
    f = interp1d(x, y, kind='linear', fill_value='extrapolate')
    ynew = f(xnew)
    ynew[ynew < 0] = 0
    return ynew


async def process_train_gen(get_conn):
    query = '''
        select distinct
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
    prev_key = None
    async for row in get_conn.execute(query):
        current_key = '|'.join([row.client_name, row.vin])

        if prev_key and current_key != prev_key:
            if len(group_values) > 1:
                x, y = tuple(), tuple()
                for group_values_line in group_values:
                    x += (group_values_line['mmm'], )
                    y += (group_values_line['odometer'], )

                x_new = tuple(i for i in range(1, x[-1]+1))
                y_new = interpolate(x, y, x_new)
                km_arr = np.diff(y_new)
                group_values[-1]['mean_km'] = np.mean(km_arr[LAG:])
                yield group_values[-1]
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


async def get_postgres_engine(loop, db):
    engine = await aiopg.sa.create_engine(**db, loop=loop)
    return engine


async def main(loop, db_config):
    pg_get = await get_postgres_engine(loop, db_config)
    async with pg_get.acquire() as conn_get:
        result_values = [i async for i in process_train_gen(conn_get)]

        print('Number of values {}'.format(len(result_values)))
        with open(OUT_FILE, 'w') as f_out:
            json.dump(result_values, f_out)

    print('JSON file ready!')


if __name__ == '__main__':
    db_conf = {'database': 'test',
               'host': 'localhost',
               'user': 'postgres',
               'password': '123',
               'port': '5432'}

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(loop, db_conf))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
