INSERT INTO trucker.store_sales_flat
  (id, quantity, net_paid, net_profit, net_paid_inc_tax, ext_discount_amt,
   sale_year, sale_month, store_name, store_state, customer_last_name,
   item_category, item_brand, deleted)
SELECT r.id,
       argMaxState(r.quantity::Int32,         now64()),
       argMaxState(r.net_paid::Float64,        now64()),
       argMaxState(r.net_profit::Float64,      now64()),
       argMaxState(r.net_paid_inc_tax::Float64, now64()),
       argMaxState(r.ext_discount_amt::Float64, now64()),
       argMaxState(r.sale_year::Int32,          now64()),
       argMaxState(r.sale_month::Int32,         now64()),
       argMaxState(r.store_name,               now64()),
       argMaxState(r.store_state,              now64()),
       argMaxState(r.customer_last_name,       now64()),
       argMaxState(r.item_category,            now64()),
       argMaxState(r.item_brand,               now64()),
       argMaxState({{ .operation | eq "delete" }}, now64())
FROM {{ .rows }}
GROUP BY id
