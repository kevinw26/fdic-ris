#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  4 11:05:00 2025

@author: ifly6
"""
import gc
import re
import zipfile as zf
from glob import glob
from os import path

import pandas as pd
from pandas._libs.tslibs.parsing import DateParseError
from tqdm import tqdm


def __list_files(directory=''):
    zips = glob(path.join(directory, 'ris*.zip'))
    return {_f: zf.ZipFile(_f).namelist() for _f in zips}


def __get_columns(the_zip, subfile):
    temp_df = pd.read_csv(
        zf.ZipFile(the_zip).open(subfile), nrows=10, low_memory=False)
    return set(temp_df.columns)


def __parse_date(s):
    try:
        return pd.to_datetime(s)

    except DateParseError as original_error:
        for dt_format in ['%Y%m', '%y%m']:
            try:
                return pd.to_datetime(s, format=dt_format)
            except DateParseError as e:
                _e = e
        raise _e from original_error


def load(dt, columns=[], required=['CERT', 'CALLYMD'], use_tqdm=False):
    # add required columns if not present, to the front
    for _c in required:
        if _c.upper() not in [i.upper() for i in columns]:
            columns = [_c] + columns

    # take date format in pattern '2020Q1' or '2020-03-31' or '2020-03'
    q_end = dt if isinstance(dt, pd.Timestamp) else None
    if q_end is None and re.match(r'\d{4}[qQ](1|2|3|4)', dt):
        q_end = pd.Timestamp(dt) + pd.offsets.QuarterEnd()

    if q_end is None and re.match(r'\d{4}-?(0?3|0?6|0?9|12)', dt):
        q_end = pd.Timestamp(dt) + pd.offsets.QuarterEnd()

    if q_end is None and re.match(r'\d{4}-?\d{2}-?d\{2}', dt):
        q_end = pd.Timestamp(dt) + pd.offsets.QuarterEnd()

    if q_end is None:
        raise ValueError(f'cannot parse date {dt}')

    # find the corresponding entries
    matches = []
    for _zf, _f_list in __list_files().items():
        for _sf in _f_list:
            the_directory = path.dirname(_sf)
            gen_date = f'ris{q_end.strftime("%y%m")}'

            if gen_date == the_directory:
                file_columns = __get_columns(_zf, _sf)
                matching_columns = [
                    c for c in columns
                    if c.upper() in [i.upper() for i in file_columns]]
                matches.append((_zf, _sf, matching_columns))

    # filter out essentially null matches
    matches = [m for m in matches if m[2] != ['CALLYMD', 'CERT']]

    # remove unindexable matches
    matches = [
        m for m in matches if 'CALLYMD' in m[2] and 'CERT' in m[2]]

    def generator():
        for _zf, _sf, _cols in matches:
            # print(_zf, _sf, _cols)
            try:
                df = pd.read_csv(
                    zf.ZipFile(_zf).open(_sf), low_memory=False, thousands=',',
                    usecols=_cols, parse_dates=['CALLYMD'])
            except UnicodeDecodeError:
                df = pd.read_csv(
                    zf.ZipFile(_zf).open(_sf), low_memory=False, thousands=',',
                    usecols=_cols, parse_dates=['CALLYMD'], encoding='latin-1')

            yield df.set_index(['CALLYMD', 'CERT'])

    return pd.concat(
        tqdm(generator(), total=len(matches)) if use_tqdm else generator(),
        axis=1, join='inner')  # expands diagonally


def load_range(s, e, columns, use_tqdm=False):
    if isinstance(s, str):
        s = __parse_date(s)
    if isinstance(e, str):
        e = __parse_date(e)

    quarter_ends = pd.date_range(start=s, end=e, freq='QE')
    _gen = (load(q, columns) for q in quarter_ends)

    to_return = pd.concat(
        tqdm(_gen, total=len(quarter_ends)) if use_tqdm else _gen,
        axis=0)
    gc.collect()
    return to_return


if __name__ == '__main__':

    # profile this
    load_range(
        '198403', '198612', ['DEPDOM', 'ASSET', 'EQ', 'NAME'], use_tqdm=True)
