#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  3 20:40:20 2025

@author: ifly6
"""
import contextlib
import re
import sqlite3 as sql
import zipfile as zf
from glob import glob
from os.path import basename

import pandas as pd
import polars as pl
from tqdm import tqdm


def __list_tables(conn):
    tt = pd \
        .read_sql_query('SELECT name FROM sqlite_master WHERE type="table"', 
                        conn) \
        ['name'].values
    return set(tt)


def save_df(df, sql_f, part: str = None, mitigate=False, parquet=False):

    # check that the names are permitted
    permitted = ['cdi', 'stru', 'fts', 'rat']
    if part.lower() not in permitted:
        raise ValueError(f'part {part} not in permitted list {permitted}')

    assert isinstance(df.index, pd.RangeIndex), 'non-numeric indices not okay'
    df.reset_index(drop=True, inplace=True)
        
    # kevin.wong. 2024-04-03. by default, max columns on sqlite is 2_000. but
    # ris has at least 4_824 columns
    with sql.connect(sql_f) as conn:

        # determine if the table exists, if it does, do further checks
        tables = __list_tables(conn)
        if part.lower() in (i.lower() for i in tables):

            # check that the database does not have that quarter and cert yet
            db_pairs = pd.read_sql_query(
                f'select distinct CERT, CALLYMD from {part}',
                conn, parse_dates=['CALLYMD']).rename(columns=str.upper)
            df_pairs = df[['CERT', 'CALLYMD']].drop_duplicates()

            _mrg = df_pairs.merge(
                db_pairs, on=['CERT', 'CALLYMD'], how='left', indicator=True,
                suffixes=('', '_db'))
            inner = _mrg.loc[_mrg['_merge'] == 'both', ['CERT', 'CALLYMD']]
            if len(inner) != 0:
                if len(db_pairs) == len(df_pairs):
                    # this is probably identical to something already saved
                    return
                
                if mitigate is False:
                    raise ValueError(
                        f'found {len(inner)} cert-callymds already saved')

                df = df[~df.index.isin(inner.index)]  # remove matches and save

        try:
            df.to_sql(con=conn, name=part, if_exists='append')
            # could pass chunksize but not necessary; one quarter doesn't have
            # that many rows
            
        except sql.OperationalError:
            # to solve new columns eg "table CDI has no column named AASLRIND"
            old_df = pd.read_sql_query(
                f'select * from {part}', conn, parse_dates=['CALLYMD'])
            
            # concatenate the old and new data; then save it
            # could run into memory problems?
            new_df = pd.concat([old_df, df])
            new_df.to_sql(
                con=conn, name=part, if_exists='replace', chunksize=10_000)


def create_indices(
        sql_f, columns=['CERT', 'CALLYMD', 'RSSDID', 'RSSDHCD', 'RSSDHCR']):
    ...


if __name__ == '__main__':
    
    assert False, 'this doesn\'t work'

    files = sorted(glob('ris*.zip'))
    for _f in files:

        zip_f = zf.ZipFile(_f)
        csvs = zip_f.namelist()  # returns a flat list of all items

        # create csv groups
        all_csvs = pd.Series(csvs, name='z_path').to_frame()
        all_csvs[['dir', 'name']] = all_csvs['z_path'].str.split(
            '/', expand=True)

        # iterate over the csvs
        csv_groups = \
            [df['z_path'].values for _, df in all_csvs.groupby('dir')]
        for csvs in (_pb := tqdm(csv_groups)):

            for _c in (i for i in csvs if 'merg' not in i.lower()):

                _pb.set_description(f'loading csv {basename(_c)}')
                try:
                    part = pd.read_csv(
                        zip_f.open(_c), parse_dates=['CALLYMD'],
                        low_memory=False, thousands=',')
                except UnicodeDecodeError:
                    part = pd.read_csv(
                        zip_f.open(_c), parse_dates=['CALLYMD'],
                        low_memory=False, thousands=',', encoding='latin-1')

                part.rename(columns=str.upper, inplace=True)
                part_nm = re.search(
                    r'[a-z]+(?=\d+\.csv$)', _c, flags=re.IGNORECASE).group(0)

                _pb.set_description(f'saving csv {basename(_c)}')
                save_df(part, 'ris.db', part=part_nm)

    # create sql indices
    create_indices('ris.db')
