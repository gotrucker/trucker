INSERT INTO trucker.customer_ch
  (id, customer_id, current_cdemo_sk, current_hdemo_sk, current_addr_sk,
   first_shipto_date_sk, first_sales_date_sk, salutation, first_name, last_name,
   preferred_cust_flag, birth_day, birth_month, birth_year, birth_country,
   login, email_address, last_review_date_sk, deleted)
SELECT r.id,
       argMaxState(r.customer_id,                 now64()),
       argMaxState(r.current_cdemo_sk::Int32,     now64()),
       argMaxState(r.current_hdemo_sk::Int32,     now64()),
       argMaxState(r.current_addr_sk::Int32,      now64()),
       argMaxState(r.first_shipto_date_sk::Int32, now64()),
       argMaxState(r.first_sales_date_sk::Int32,  now64()),
       argMaxState(r.salutation,                  now64()),
       argMaxState(r.first_name,                  now64()),
       argMaxState(r.last_name,                   now64()),
       argMaxState(r.preferred_cust_flag,         now64()),
       argMaxState(r.birth_day::Int32,            now64()),
       argMaxState(r.birth_month::Int32,          now64()),
       argMaxState(r.birth_year::Int32,           now64()),
       argMaxState(r.birth_country,               now64()),
       argMaxState(r.login,                       now64()),
       argMaxState(r.email_address,               now64()),
       argMaxState(r.last_review_date_sk::Int32,  now64()),
       argMaxState({{ .operation | eq "delete" }}, now64())
FROM {{ .rows }}
GROUP BY id
