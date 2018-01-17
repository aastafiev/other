# -*- coding: utf-8 -*-

import sqlalchemy as sa
# import sqlalchemy.dialects


metadata = sa.MetaData(schema='public')


pre_train_filtered = sa.Table('client_time_series_pre_train_filtered', metadata,
                              sa.Column('region', sa.String(1024)),
                              sa.Column('bir', sa.String(1024)),
                              sa.Column('client_name', sa.String(1024)),
                              sa.Column('vin', sa.String(1024)),
                              sa.Column('model', sa.String(1024)),
                              sa.Column('yyy', sa.Integer),
                              sa.Column('mmm', sa.Integer),
                              # sa.Column('work_type', sa.String(1024)),
                              # sa.Column('work_code', sa.String(1024)),
                              sa.Column('odometer', sa.Integer),
                              sa.Column('presence', sa.Integer),
                              sa.Column('r_n', sa.Integer)
                              )


interpolated = sa.Table('client_time_series_interpolated', metadata,
                        sa.Column('region', sa.String(1024)),
                        sa.Column('bir', sa.String(1024)),
                        sa.Column('client_name', sa.String(1024)),
                        sa.Column('vin', sa.String(1024)),
                        sa.Column('model', sa.String(1024)),
                        sa.Column('yyy', sa.Integer),
                        sa.Column('mmm', sa.Integer),
                        sa.Column('work_type', sa.String(1024)),
                        sa.Column('work_code', sa.String(1024)),
                        sa.Column('odometer', sa.Integer),
                        sa.Column('km', sa.Integer),
                        sa.Column('presence', sa.Integer, nullable=False),
                        sa.Column('exp_work_type', sa.String(1024)),
                        sa.Column('r_n', sa.Integer, nullable=False),
                        )


train = sa.Table('client_time_series_train', metadata,
                 sa.Column('region', sa.String(1024)),
                 sa.Column('bir', sa.String(1024)),
                 sa.Column('client_name', sa.String(1024)),
                 sa.Column('vin', sa.String(1024)),
                 sa.Column('model', sa.String(1024)),
                 sa.Column('yyy', sa.Integer),
                 sa.Column('mmm', sa.Integer),
                 sa.Column('work_type', sa.String(1024)),
                 sa.Column('work_code', sa.String(1024)),
                 sa.Column('odometer', sa.Integer),
                 sa.Column('km', sa.Integer),
                 sa.Column('presence', sa.Integer, nullable=False),
                 sa.Column('exp_work_type', sa.String(1024))
                 )


test = sa.Table('client_time_series_test', metadata,
                sa.Column('region', sa.String(1024)),
                sa.Column('bir', sa.String(1024)),
                sa.Column('client_name', sa.String(1024)),
                sa.Column('vin', sa.String(1024)),
                sa.Column('model', sa.String(1024)),
                sa.Column('yyy', sa.Integer),
                sa.Column('mmm', sa.Integer),
                sa.Column('work_type', sa.String(1024)),
                sa.Column('work_code', sa.String(1024)),
                sa.Column('odometer', sa.Integer),
                sa.Column('km', sa.Integer),
                sa.Column('presence', sa.Integer, nullable=False),
                sa.Column('exp_work_type', sa.String(1024))
                )
