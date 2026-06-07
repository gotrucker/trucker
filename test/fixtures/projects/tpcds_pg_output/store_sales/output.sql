INSERT INTO trucker.store_sales_pg
  (id, quantity, net_paid, net_profit, net_paid_inc_tax, ext_discount_amt,
   sale_year, sale_month, store_name, store_state, customer_last_name,
   item_category, item_brand, deleted)
SELECT r.id,
       r.quantity::int,
       r.net_paid::float8,
       r.net_profit::float8,
       r.net_paid_inc_tax::float8,
       r.ext_discount_amt::float8,
       r.sale_year::int,
       r.sale_month::int,
       r.store_name,
       r.store_state,
       r.customer_last_name,
       r.item_category,
       r.item_brand,
       {{ .operation | eq "delete" }}
FROM {{ .rows }}
ON CONFLICT (id) DO UPDATE SET
  quantity           = EXCLUDED.quantity,
  net_paid           = EXCLUDED.net_paid,
  net_profit         = EXCLUDED.net_profit,
  net_paid_inc_tax   = EXCLUDED.net_paid_inc_tax,
  ext_discount_amt   = EXCLUDED.ext_discount_amt,
  sale_year          = EXCLUDED.sale_year,
  sale_month         = EXCLUDED.sale_month,
  store_name         = EXCLUDED.store_name,
  store_state        = EXCLUDED.store_state,
  customer_last_name = EXCLUDED.customer_last_name,
  item_category      = EXCLUDED.item_category,
  item_brand         = EXCLUDED.item_brand,
  deleted            = EXCLUDED.deleted
