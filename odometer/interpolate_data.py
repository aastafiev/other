#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import aiopg.sa
import sqlalchemy as sa

from scipy.interpolate import interp1d
import numpy as np

import odometer.db as db


def filter_x_y(x, y):
    # Check values
    y_local = np.array(y)
    x_local = np.array(x)
    for i in range(0, y_local.size - 1):
        for j in range(i + 1, y_local.size):
            if y_local[i] > y_local[j]:
                y_local[j] = 0
    zero_idx = np.where(y_local == 0)
    if zero_idx[0].size:
        y_local = np.delete(y_local, zero_idx)
        x_local = np.delete(x_local, zero_idx)

    return x_local, y_local


def interpolate(x, y, xnew):
    f = interp1d(x, y, kind='linear', fill_value='extrapolate')
    ynew = f(xnew)
    ynew[ynew < 0] = 0
    return ynew


def calc_exp_work_type(value):
    work_types = {'M-15': (12000, 18000),
                  'M-30': (28000, 32000),
                  'M-40': (39000, 41000),
                  'M-45': (43500, 48500),
                  'M-50': (49000, 51000),
                  'M-60': (58000, 62000),
                  'M-70': (69000, 71500),
                  'M-75': (73000, 77500),
                  'M-80': (79000, 81000),
                  'M-90': (88500, 92000),
                  'M-100': (99000, 101500),
                  'M-105': (103500, 107000),
                  'M-110': (109000, 111000),
                  'M-120': (119000, 121500),
                  'M-130': (129000, 131500),
                  'M-135': (134000, 138000),
                  'M-140': (139000, 142000),
                  'M-150': (148000, 152500)}

    for key, (segment_start, segment_end) in work_types.items():
        if segment_start <= value <= segment_end:
            return key

    return None


# async def interpolate_data(get_conn, set_conn):
async def interpolate_data_gen(get_conn):
    query = sa.select([db.pre_train_filtered]).order_by(db.pre_train_filtered.c.client_name,
                                                        db.pre_train_filtered.c.vin,
                                                        db.pre_train_filtered.c.yyy,
                                                        db.pre_train_filtered.c.mmm)

    new_values = []
    insert_values = []
    x = tuple()
    x_new = tuple()
    y = tuple()
    prev_key = None
    rows_counter = 10000
    total_rows = 0
    async for row in get_conn.execute(query):
        current_key = '|'.join([row.client_name, row.vin])

        if prev_key and current_key != prev_key:
            filtered_x_y = filter_x_y(x, y)
            if filtered_x_y[0].size > 1 and filtered_x_y[1].size > 1:  # Remove clients with the only one presence
                rows_counter -= 1
                # x = filtered_x_y[0], y = filtered_x_y[1]
                y_new_arr = interpolate(filtered_x_y[0], filtered_x_y[1], x_new)
                km_arr = np.append([-1], np.diff(y_new_arr))
                assert y_new_arr.size == len(new_values)
                for new_values_line in new_values:
                    # new_values_line['r_n'] = int(r_n)
                    r_n = new_values_line['r_n']
                    new_odometer = int(round(y_new_arr[r_n - 1], 0))
                    new_values_line['odometer'] = new_odometer
                    new_values_line['exp_work_type'] = calc_exp_work_type(new_odometer)
                    new_values_line['km'] = int(round(km_arr[r_n - 1], 0)) if km_arr[r_n - 1] != -1 else None

                insert_values.extend(new_values)
                if rows_counter == 0:
                    print('Insert rows {}'.format(len(insert_values)))
                    yield insert_values
                    # await set_conn.execute(db.interpolated.insert().values(insert_values))
                    rows_counter = 10000
                    total_rows += len(insert_values)
                    insert_values = []
            new_values = []

            x = tuple()
            y = tuple()
            x_new = tuple()

        new_values.append({'region': row.region,
                           'bir': row.bir,
                           'client_name': row.client_name,
                           'vin': row.vin,
                           'model': row.model,
                           'yyy': row.yyy,
                           'mmm': row.mmm,
                           'presence': row.presence,
                           'r_n': row.r_n})

        if row.odometer != 0:
            x += (row.r_n,)
            y += (row.odometer,)
        x_new += (row.r_n,)

        prev_key = current_key

    if insert_values:
        print('Insert rows {}'.format(len(insert_values)))
        yield insert_values
        # await set_conn.execute(db.interpolated.insert().values(insert_values))
        total_rows += len(insert_values)
        print('Total inserted rows {}'.format(total_rows))


async def get_postgres_engine(loop, db):
    return await aiopg.sa.create_engine(**db, loop=loop)


async def main(loop, db_config):
    async with await get_postgres_engine(loop, db_config) as pg_get, await get_postgres_engine(loop, db_config) as pg_set:
        async with pg_get.acquire() as conn_get, pg_set.acquire() as conn_set:
            # await interpolate_data(conn_get, conn_set)
            async for values in interpolate_data_gen(conn_get):
                await conn_set.execute(db.interpolated.insert().values(values))


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
