#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  4 16:33:24 2025
@author: kevin.wong
"""
import polars as pl
import pandas as pd

if __name__ == '__main__':

    def drop_drop(df):
        return df.loc[:, ~df.columns.str.endswith('_DROP')]

    # -------------------------------------------------------------------------
    # scan parquet super fast to get just a few columns

    comparison = (
        pl.scan_parquet('../../pq/ris*.pq')
        .select(['CALLYMD', 'CERT', 'RSSDID', 'RSSDHCD', 'RSSDHCR'])
        .with_columns(
            RSSDHCR=pl.col('RSSDHCR').replace(0, None),
            RSSDHCD=pl.col('RSSDHCD').replace(0, None),
            RSSDID=pl.col('RSSDID').replace(0, None)
        )
        .with_columns(
            top_RSSD=(
                pl.col('RSSDHCR')
                .fill_null(pl.col('RSSDHCD'))
                .fill_null(pl.col('RSSDID'))
                .cast(pl.Int64)
            ))
        .collect()
    )

    # -------------------------------------------------------------------------
    # now merge to find differences

    # some of the early data has top RSSD of 0
    parents = comparison.to_pandas()
    parents_offset = parents.copy()
    parents_offset['CALLYMD'] = parents_offset['CALLYMD'] - \
        pd.offsets.QuarterEnd(1)

    # use the two versions
    merged = pd.merge(
        parents, parents_offset,
        on=['CALLYMD', 'CERT'], suffixes=['', '_next'], how='left')

    # sort and drop last
    merged = merged[merged['CALLYMD'] != merged['CALLYMD'].max()] \
        .sort_values(['CALLYMD', 'CERT'])

    # find differences only
    differenced = merged[merged['top_RSSD'] != merged['top_RSSD_next']] \
        .assign(CALLYMD_next=lambda d: d['CALLYMD'] + pd.offsets.QuarterEnd(1))

    # calculate how many banks are in each top rssd; attach on CALLYMD_next
    post_count = parents.groupby(['top_RSSD', 'CALLYMD'])['CERT'] \
        .count().rename('top_RSSD_next_bankcnt')
    differenced = differenced.merge(
        post_count.reset_index(),
        left_on=['top_RSSD_next', 'CALLYMD_next'],
        right_on=['top_RSSD', 'CALLYMD'],
        how='left', suffixes=('', '_DROP'), validate='m:1')
    differenced = drop_drop(differenced)

    # determine if a bank exists at CALLYMD_next or not
    _e = parents.set_index(['CALLYMD', 'CERT']) \
        .assign(exists_next=True).iloc[:, -1] \
        .reindex(pd.MultiIndex.from_product(
            [
                parents['CALLYMD'].unique(),
                parents['CERT'].unique()
            ],
            names=['CALLYMD', 'CERT']
        ), fill_value=False).reset_index()
    differenced = differenced.merge(
        _e, left_on=['CALLYMD_next', 'CERT'], right_on=['CALLYMD', 'CERT'],
        how='left', suffixes=('', '_DROP'))
    differenced = drop_drop(differenced)

    # export
    differenced.to_parquet('kw01_top_rssd_changes.pq')
