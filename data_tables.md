# Data Tables to Create the Cost of Travel Index

# 

**transaction_tourism: This contains the individual transactions used in the index.**


## Table Name: Transaction Tourism

Table URI: `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism`


* txid - STRING: A unique transaction ID
  mtid - STRING: A merchant ID used as a foreign key to the merchant_tourism table to bring in the location of the merchant and the MCC codes needed for classification.
  merch_h3_13 - STRING - the H3 Geoindex of the approximate merchant location
  membccid - STRING	- Unique cardhoder ID - Foreign key to the card_tourism table for cardholder information
  card_h3_13 - STRING	 - the H3 Geoindex of the approximate cardholder's home location
  trans_date - DATE - localized date of the transaction
  trans_amount - FLOAT - The Raw transaction amount {Use this for the Cost of Travel Index}
  norm_spend - FLOAT - A normalized spending value used for estimating the total volume of transactions within a location (do not use this for the Cost of Travel Index)
  trans_distance - FLOAT - The distance in miles between the cardholder's home location and the merchant's location. A distance of greater than 60 miles is used as "Visitor Spending".
  delivery_date - DATE - the data delivery date to Zartico
  ref_date - DATE - The data shipment reference date


## Table Name: Merchant Tourism

Table URI : `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism`

| Field_name | Type |    |
|----|----|----|
| mtid | STRING |    |
| merch_desc | STRING |    |
| mcc | STRING |    |
| merch_type | INT64 |    |
| merch_city | STRING |    |
| merch_state | STRING |    |
| merch_zip | STRING |    |
| merch_country | STRING |    |
| z_country | STRING |    |
| h3_13 | STRING |    |
| delivery_date | DATE |    |
| ref_date | DATE |    |

Note: `ref_date` is the partitioning column for this table.


## Table name: admin_geo_reference

Table URI: `data-reference.Geo_Reference.admin_geo_reference`

| Field_name | Type |
|----|----|
| client_id | STRING |
| country | STRING |
| country_id | INT64 |
| country_abbr | STRING |
| admin1 | STRING |
| admin1_id | INT64 |
| admin1_abbr | STRING |
| admin2 | STRING |
| admin2_id | INT64 |
| admin3 | STRING |
| admin3_id | INT64 |
| admin_pk | INT64 |
| processed_timestamp | TIMESTAMP |


