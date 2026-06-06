DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
DELETE FROM pg_publication;
DROP PUBLICATION IF EXISTS trucker_trucker3;
SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;

CREATE TABLE public.store_sales (
  ss_sold_date_sk       int,
  ss_sold_time_sk       int,
  ss_item_sk            int          NOT NULL,
  ss_customer_sk        int,
  ss_cdemo_sk           int,
  ss_hdemo_sk           int,
  ss_addr_sk            int,
  ss_store_sk           int,
  ss_promo_sk           int,
  ss_ticket_number      bigint       NOT NULL,
  ss_quantity           int,
  ss_wholesale_cost     numeric(7,2),
  ss_list_price         numeric(7,2),
  ss_sales_price        numeric(7,2),
  ss_ext_discount_amt   numeric(7,2),
  ss_ext_sales_price    numeric(7,2),
  ss_ext_wholesale_cost numeric(7,2),
  ss_ext_list_price     numeric(7,2),
  ss_ext_tax            numeric(7,2),
  ss_coupon_amt         numeric(7,2),
  ss_net_paid           numeric(7,2),
  ss_net_paid_inc_tax   numeric(7,2),
  ss_net_profit         numeric(7,2),
  PRIMARY KEY (ss_item_sk, ss_ticket_number)
);

CREATE TABLE public.date_dim (
  d_date_sk             int          NOT NULL PRIMARY KEY,
  d_date_id             char(16)     NOT NULL,
  d_date                date         NOT NULL,
  d_month_seq           int          NOT NULL,
  d_week_seq            int          NOT NULL,
  d_quarter_seq         int          NOT NULL,
  d_year                int          NOT NULL,
  d_dow                 int          NOT NULL,
  d_moy                 int          NOT NULL,
  d_dom                 int          NOT NULL,
  d_qoy                 int          NOT NULL,
  d_fy_year             int          NOT NULL,
  d_fy_quarter_seq      int          NOT NULL,
  d_fy_week_seq         int          NOT NULL,
  d_day_name            varchar(9)   NOT NULL,
  d_quarter_name        char(6)      NOT NULL,
  d_holiday             char(1)      NOT NULL DEFAULT 'N',
  d_weekend             char(1)      NOT NULL DEFAULT 'N',
  d_following_holiday   char(1)      NOT NULL DEFAULT 'N',
  d_first_dom           int          NOT NULL,
  d_last_dom            int          NOT NULL,
  d_same_day_ly         int          NOT NULL,
  d_same_day_lq         int          NOT NULL,
  d_current_day         char(1)      NOT NULL DEFAULT 'N',
  d_current_week        char(1)      NOT NULL DEFAULT 'N',
  d_current_month       char(1)      NOT NULL DEFAULT 'N',
  d_current_quarter     char(1)      NOT NULL DEFAULT 'N',
  d_current_year        char(1)      NOT NULL DEFAULT 'N'
);

CREATE TABLE public.store (
  s_store_sk            int          NOT NULL PRIMARY KEY,
  s_store_id            char(16)     NOT NULL,
  s_rec_start_date      date         NOT NULL DEFAULT '1900-01-01',
  s_rec_end_date        date         NOT NULL DEFAULT '9999-12-31',
  s_closed_date_sk      int          NOT NULL DEFAULT 0,
  s_store_name          varchar(50)  NOT NULL DEFAULT '',
  s_number_employees    int          NOT NULL DEFAULT 0,
  s_floor_space         int          NOT NULL DEFAULT 0,
  s_hours               char(20)     NOT NULL DEFAULT '',
  s_manager             varchar(40)  NOT NULL DEFAULT '',
  s_market_id           int          NOT NULL DEFAULT 0,
  s_geography_class     varchar(100) NOT NULL DEFAULT '',
  s_market_desc         varchar(100) NOT NULL DEFAULT '',
  s_market_manager      varchar(40)  NOT NULL DEFAULT '',
  s_division_id         int          NOT NULL DEFAULT 0,
  s_division_name       varchar(50)  NOT NULL DEFAULT '',
  s_company_id          int          NOT NULL DEFAULT 0,
  s_company_name        varchar(50)  NOT NULL DEFAULT '',
  s_street_number       varchar(10)  NOT NULL DEFAULT '',
  s_street_name         varchar(60)  NOT NULL DEFAULT '',
  s_street_type         char(15)     NOT NULL DEFAULT '',
  s_suite_number        char(10)     NOT NULL DEFAULT '',
  s_city                varchar(60)  NOT NULL DEFAULT '',
  s_county              varchar(30)  NOT NULL DEFAULT '',
  s_state               char(2)      NOT NULL DEFAULT '',
  s_zip                 char(10)     NOT NULL DEFAULT '',
  s_country             varchar(20)  NOT NULL DEFAULT '',
  s_gmt_offset          numeric(5,2) NOT NULL DEFAULT 0,
  s_tax_precentage      numeric(5,2) NOT NULL DEFAULT 0
);

CREATE TABLE public.customer (
  c_customer_sk         int          NOT NULL PRIMARY KEY,
  c_customer_id         char(16)     NOT NULL,
  c_current_cdemo_sk    int          NOT NULL DEFAULT 0,
  c_current_hdemo_sk    int          NOT NULL DEFAULT 0,
  c_current_addr_sk     int          NOT NULL DEFAULT 0,
  c_first_shipto_date_sk int         NOT NULL DEFAULT 0,
  c_first_sales_date_sk  int         NOT NULL DEFAULT 0,
  c_salutation          char(10)     NOT NULL DEFAULT '',
  c_first_name          varchar(20)  NOT NULL DEFAULT '',
  c_last_name           varchar(30)  NOT NULL DEFAULT '',
  c_preferred_cust_flag char(1)      NOT NULL DEFAULT 'N',
  c_birth_day           int          NOT NULL DEFAULT 0,
  c_birth_month         int          NOT NULL DEFAULT 0,
  c_birth_year          int          NOT NULL DEFAULT 0,
  c_birth_country       varchar(20)  NOT NULL DEFAULT '',
  c_login               char(13)     NOT NULL DEFAULT '',
  c_email_address       varchar(50)  NOT NULL DEFAULT '',
  c_last_review_date_sk int          NOT NULL DEFAULT 0
);

CREATE TABLE public.item (
  i_item_sk             int          NOT NULL PRIMARY KEY,
  i_item_id             char(16)     NOT NULL,
  i_rec_start_date      date         NOT NULL DEFAULT '1900-01-01',
  i_rec_end_date        date         NOT NULL DEFAULT '9999-12-31',
  i_item_desc           varchar(200) NOT NULL DEFAULT '',
  i_current_price       numeric(7,2) NOT NULL DEFAULT 0,
  i_wholesale_cost      numeric(7,2) NOT NULL DEFAULT 0,
  i_brand_id            int          NOT NULL DEFAULT 0,
  i_brand               char(50)     NOT NULL DEFAULT '',
  i_class_id            int          NOT NULL DEFAULT 0,
  i_class               char(50)     NOT NULL DEFAULT '',
  i_category_id         int          NOT NULL DEFAULT 0,
  i_category            char(50)     NOT NULL DEFAULT '',
  i_manufact_id         int          NOT NULL DEFAULT 0,
  i_manufact            char(50)     NOT NULL DEFAULT '',
  i_size                char(20)     NOT NULL DEFAULT '',
  i_formulation         char(20)     NOT NULL DEFAULT '',
  i_color               char(20)     NOT NULL DEFAULT '',
  i_units               char(10)     NOT NULL DEFAULT '',
  i_container           char(10)     NOT NULL DEFAULT '',
  i_manager_id          int          NOT NULL DEFAULT 0,
  i_product_name        char(50)     NOT NULL DEFAULT ''
);

ALTER TABLE public.store_sales   REPLICA IDENTITY FULL;
ALTER TABLE public.date_dim      REPLICA IDENTITY FULL;
ALTER TABLE public.store         REPLICA IDENTITY FULL;
ALTER TABLE public.customer      REPLICA IDENTITY FULL;
ALTER TABLE public.item          REPLICA IDENTITY FULL;

CREATE INDEX ON public.date_dim (d_date_sk);
CREATE INDEX ON public.store (s_store_sk);
CREATE INDEX ON public.customer (c_customer_sk);
CREATE INDEX ON public.item (i_item_sk);
