#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
# import csv
import aiopg.sa
import sqlalchemy as sa

from scipy.interpolate import interp1d
import numpy as np

# import settings as st

import odometer.db as db


# INPUT_FILE = os.path.join(st.PROJECT_DIR, 'predict_service', 'data', '111_201712181057.csv')


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
    test_values = []
    x = tuple()
    # xnew = tuple()
    y = tuple()
    prev_key = None
    rows_counter = 10000
    total_train_rows = 0
    total_test_rows = 0
    for row in await get_conn.execute(query):
        current_key = '|'.join([row.client_name, row.vin])

        if prev_key and current_key != prev_key:
            is_last_exp_work_type = False
            added_test_line = False
            test_line = None
            rows_counter -= 1
            for new_values_line in group_values[::-1]:
                if not added_test_line:
                    if new_values_line['exp_work_type'] and not is_last_exp_work_type:
                        is_last_exp_work_type = True
                        test_line = new_values_line
                    elif new_values_line['exp_work_type'] and is_last_exp_work_type:
                        test_line = new_values_line
                    elif not new_values_line['exp_work_type'] and is_last_exp_work_type:
                        test_values.append(test_line)
                        added_test_line = True
                        if new_values_line['presence']:
                            train_values.append(new_values_line)
                elif new_values_line['presence']:
                    train_values.append(new_values_line)

            if rows_counter == 0:
                print('Insert TRAIN rows {}'.format(len(train_values)))
                await set_conn.execute(db.train.insert().values(train_values))
                print('Insert TEST rows {}'.format(len(test_values)))
                await set_conn.execute(db.test.insert().values(test_values))
                rows_counter = 10000
                total_train_rows += len(train_values)
                total_test_rows += len(test_values)
                train_values = []
                test_values = []
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

    if train_values:
        print('Insert TRAIN rows {}'.format(len(train_values)))
        await set_conn.execute(db.train.insert().values(train_values))
        total_train_rows += len(train_values)
        print('Total TRAIN inserted rows {}'.format(total_train_rows))
    if test_values:
        print('Insert TEST rows {}'.format(len(test_values)))
        await set_conn.execute(db.test.insert().values(test_values))
        total_test_rows += len(test_values)
        print('Total TEST inserted rows {}'.format(total_test_rows))


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
