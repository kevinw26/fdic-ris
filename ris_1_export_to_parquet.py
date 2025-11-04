#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 19:14:30 2025
@author: kevin.wong
"""
import re
import os
import zipfile as zf
from concurrent.futures import ProcessPoolExecutor, wait
from glob import glob
from os.path import basename, exists, join

import pandas as pd
from tqdm import tqdm


def _read(the_zp, the_subfile):
    handle = zf.ZipFile(the_zp).open(the_subfile)

    try:
        part = pd.read_csv(
            handle, parse_dates=['CALLYMD'], low_memory=False, thousands=',')
    except UnicodeDecodeError:
        part = pd.read_csv(
            handle, parse_dates=['CALLYMD'], low_memory=False, thousands=',',
            encoding='latin-1')

    part.rename(columns=str.upper, inplace=True)
    part.set_index(['CALLYMD', 'CERT'], inplace=True)
    return part


if __name__ == '__main__':

    files = sorted(glob('ris*.zip'))
    for _f in files:

        zip_f = zf.ZipFile(_f)
        subfiles = zip_f.namelist()  # returns a flat list of all items

        # create csv groups
        all_csvs = pd.Series(subfiles, name='z_path').to_frame()
        all_csvs[['dir', 'name']] = all_csvs['z_path'] \
            .str.strip().str.split(r'/', expand=True)[
                lambda d: d[1].str.lower().str.contains('csv', na=False)] \
            .dropna(axis=1)

        # iterate over the csvs
        csv_groups = [df['z_path'].values for _, df in all_csvs.groupby('dir')]
        for csvs in (_pb := tqdm(csv_groups)):
            # derive date
            _ym = pd.to_datetime(
                re.search(r'ris(\d{4})', csvs[0]).group(1),
                format='%y%m') + pd.offsets.QuarterEnd(0)

            # skip files already created
            to_save = join('pq', f'ris_{_ym.strftime("%Y%m")}.pq')
            if exists(to_save):
                _pb.set_description(f'skipping file {to_save}')
                continue

            # load files and join on index
            with ProcessPoolExecutor(4) as ppe:
                _pb.set_description('parallel reading csv group')
                futures = [
                    ppe.submit(_read, _f, _c) for _c in
                    (i for i in csvs if 'merg' not in i.lower())]

                wait(futures)
                quarter_dfs = [f.result() for f in futures]

            quarter_df = pd.concat(quarter_dfs, axis=1)
            quarter_df = quarter_df.loc[
                :, ~quarter_df.columns.duplicated()].copy()

            # validate index has same date
            assert all(
                i == quarter_df.index[0][0] for i in
                quarter_df.index.get_level_values(0))
            assert quarter_df.columns.duplicated().sum() == 0

            # verify that the date is the same as in the file name
            _dt = quarter_df.index[0][0]
            assert _ym == _dt, 'date mismatch between file and descriptor'

            # save quarter parquet file
            _pb.set_description('saving parquet file')
            os.makedirs('pq', exist_ok=True)
            quarter_df.to_parquet(to_save, index=True, compression='zstd')
