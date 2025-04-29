#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 20:32:18 2025

@author: ifly6
"""
import polars as pl

if __name__ == '__main__':

    # scan parquet is REALLY fast
    agg = pl.scan_parquet('pq/*.pq') \
        .group_by('CALLYMD') \
        .agg(
            ASSET_SUM=pl.col('ASSET').sum(),
            DEPDOM_SUM=pl.col('DEPDOM').sum(),
            EQ_SUM=pl.col('EQ').sum(),
            COUNT=pl.col('CERT').n_unique(),
            BHC_COUNT=pl.col('RSSDHCR').fill_null(
                pl.col('RSSDID')).n_unique()) \
        .sort(by='CALLYMD') \
        .collect() \
        .with_columns(
            LEVERAGE=pl.col('ASSET_SUM') / pl.col('EQ_SUM'),
            BK_TO_BHC=pl.col('COUNT') / pl.col('BHC_COUNT'))

    # banks to bhcs plot over time
    agg[['CALLYMD', 'BK_TO_BHC']].to_pandas().set_index('CALLYMD').plot()

    # generate nationwide HHI
    hhi = pl.scan_parquet('pq/*.pq') \
        .join(
            pl.scan_parquet('pq/*.pq').group_by('CALLYMD').agg(
                DEPDOM_t=pl.col('DEPDOM').sum()),
            on='CALLYMD', how='left') \
        .filter(pl.col('CALLYMD') > pl.datetime(2011, 7, 21)) \
        .with_columns(
            MKT_SH=100 * pl.col('DEPDOM') / pl.col('DEPDOM_t'),
            BHC_ID=pl.col('RSSDHCR').fill_null(pl.col('RSSDID'))) \
        .group_by(['CALLYMD', 'BHC_ID']).agg(MKT_SH=pl.col('MKT_SH').sum()) \
        .group_by('CALLYMD').agg(HHI=pl.col('MKT_SH').pow(2).sum()) \
        .sort(by='CALLYMD') \
        .collect()
        
    hhi.to_pandas().set_index('CALLYMD').plot()