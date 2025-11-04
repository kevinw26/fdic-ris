#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  4 16:33:24 2025
@author: kevin.wong
"""
import polars as pl
import pandas as pd

if __name__ == "__main__":

    # -------------------------------------------------------------------------
    # scan parquet super fast to get just a few columns

    comparison = (
        pl.scan_parquet("../../pq/ris*.pq")
        .select(["CALLYMD", "CERT", "RSSDID", "RSSDHCD", "RSSDHCR"])
        .with_columns(
            previous_CALLYMD=pl.col("CALLYMD").dt.offset_by(
                "-1q").dt.month_end(),
            top_RSSD=(
                pl.col("RSSDHCR")
                .fill_null(pl.col("RSSDHCD"))
                .fill_null(pl.col("RSSDID"))
                .cast(pl.Int64)
            ))
        .collect()
    )

    # -------------------------------------------------------------------------
    # now merge to find differences

    parents = comparison.to_pandas()

    merged = pd.merge(
        parents.loc[
            lambda d: d['CALLYMD'] < d['CALLYMD'].max(),
            ['CERT', 'CALLYMD', 'top_RSSD']],
        parents.loc[
            lambda d: d['previous_CALLYMD'] >= d['CALLYMD'].min(),
            ['CERT', 'previous_CALLYMD', 'top_RSSD']
        ],
        left_on=['CALLYMD', 'CERT'], right_on=['previous_CALLYMD', 'CERT'],
        how='left')

    merged.sort_values(['CALLYMD', 'CERT'], inplace=True)

    differenced = merged[merged['top_RSSD_x'] != merged['top_RSSD_y']] \
        .rename(columns={
            'top_RSSD_x': 'top_RSSD', 'top_RSSD_y': 'next_top_RSSD'}) \
        .drop(columns=['previous_CALLYMD']) \
        .assign(next_CALLYMD=lambda d: d['CALLYMD'] + pd.offsets.QuarterEnd(1)) \
        .merge(
            parents[['CALLYMD', 'CERT', 'RSSDID', 'RSSDHCD', 'RSSDHCR']],
            on=['CALLYMD', 'CERT'])

    differenced.to_parquet("kw01_top_rssd_changes.pq")
    # much still needs to be done
    #   1. deal with the possibility that a bank creates for itself a BHC
    #       - possibly use BHC creation dates in NIC attr to filter?
    #       - count number of banks in the BHC as of CALLYMD?
    #   2. this is only movement of banks between BHCs by looking at diffs
    #      and does not include new banks created or banks that vanish from
    #      call reports; banks leave by FAILURE or MERGER, those can be added
    #      in via RIS merge
