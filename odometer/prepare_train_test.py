#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
# import csv
import aiopg.sa
# import sqlalchemy as sa

# from scipy.interpolate import interp1d
# import numpy as np

# import settings as st

import odometer.db as db


# INPUT_FILE = os.path.join(st.PROJECT_DIR, 'odometer', 'data', '111_201712181057.csv')


async def prepare_train_test(get_conn, set_conn):
    query = '''
        select
            ci.*
        from client_time_series_interpolated ci
        join (
            select
                client_name,
                vin
            from client_time_series_interpolated
            where
                presence = 1
                and yyy = 2017
            group by
                client_name,
                vin,
                presence
            having count(*) > 2
        ) tbl1 on ci.client_name = tbl1.client_name and ci.vin = tbl1.vin
        where 
            ci.yyy = 2017
        order by 
            ci.client_name,
            ci.vin,
            ci.yyy,
            ci.mmm;
    '''

    new_values = []
    train_values = []
    test_values = []
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
            for new_values_line in new_values[::-1]:
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
            new_values = []

        new_values.append({'region': row.region,
                           'bir': row.bir,
                           'client_name': row.client_name,
                           'vin': row.vin,
                           'model': row.model,
                           'yyy': row.yyy,
                           'mmm': row.mmm,
                           'odometer': row.odometer,
                           'presence': row.presence,
                           'exp_work_type': row.exp_work_type})
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