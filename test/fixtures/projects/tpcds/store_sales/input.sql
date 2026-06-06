SELECT COALESCE(r.ss_item_sk,       r.old__ss_item_sk)::text
    || '_'
    || COALESCE(r.ss_ticket_number, r.old__ss_ticket_number)::text AS id,
       COALESCE(r.ss_quantity,           r.old__ss_quantity,           0)         AS quantity,
       COALESCE(r.ss_net_paid,           r.old__ss_net_paid,           0.0)::float8 AS net_paid,
       COALESCE(r.ss_net_profit,         r.old__ss_net_profit,         0.0)::float8 AS net_profit,
       COALESCE(r.ss_net_paid_inc_tax,   r.old__ss_net_paid_inc_tax,   0.0)::float8 AS net_paid_inc_tax,
       COALESCE(r.ss_ext_discount_amt,   r.old__ss_ext_discount_amt,   0.0)::float8 AS ext_discount_amt,
       COALESCE(d.d_year, 0)             AS sale_year,
       COALESCE(d.d_moy,  0)             AS sale_month,
       COALESCE(s.s_store_name, '')      AS store_name,
       COALESCE(s.s_state, '')           AS store_state,
       COALESCE(c.c_last_name, '')       AS customer_last_name,
       COALESCE(i.i_category, '')        AS item_category,
       COALESCE(i.i_brand, '')           AS item_brand
FROM {{ .rows }}
LEFT JOIN public.date_dim d ON COALESCE(r.ss_sold_date_sk, r.old__ss_sold_date_sk) = d.d_date_sk
LEFT JOIN public.store    s ON COALESCE(r.ss_store_sk,     r.old__ss_store_sk)     = s.s_store_sk
LEFT JOIN public.customer c ON COALESCE(r.ss_customer_sk,  r.old__ss_customer_sk)  = c.c_customer_sk
LEFT JOIN public.item     i ON COALESCE(r.ss_item_sk,      r.old__ss_item_sk)      = i.i_item_sk
