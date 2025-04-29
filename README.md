# fdic-ris
Processed call report data from FDIC's Research Information System, in parquet form
 
## Call reports

Call reports are quarterly reports of all FDIC insured institutions' condition and income. In the current form they go back to 1984, though even earlier call report data is available.

Standard call reports can be downloaded in their raw form from the FFIEC website. However, the FDIC processes it into RIS, creating a longitudinally consistent set of variables that do not change even as underlying call report fields come in and out. Ratios of import, such as the tangible equity ratio used for bank closure, are also pre-calculated.

All of these variables (around 3,000) are available on the FDIC's FOIA archive in `.sas7bdat` and `.csv.zip` format. This repository processes them into `.pq` files which can be scanned using Pandas or Polars. An example script is given in `ris_2_polars_groupby_test.py`.
