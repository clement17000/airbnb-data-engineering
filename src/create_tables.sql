CREATE SCHEMA IF NOT EXISTS airbnb
OPTIONS(
    location="EU"
);

CREATE OR REPLACE EXTERNAL TABLE airbnb.raw_listings
OPTIONS(
    format='CSV'
    uris=['gs://airbnb-data-engineering-raw-cg/raw/listings_bdx.csv']
);