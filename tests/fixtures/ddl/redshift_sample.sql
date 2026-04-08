CREATE TABLE public.sales_order_header (
    sales_order_id   INTEGER      NOT NULL,
    customer_id      INTEGER      NOT NULL,
    territory_id     INTEGER,
    order_date       TIMESTAMP    NOT NULL,
    total_due        DECIMAL(19,4) NOT NULL,
    status           SMALLINT     NOT NULL,
    is_online_order  BOOLEAN      NOT NULL DEFAULT FALSE
);

CREATE TABLE public.sales_territory (
    territory_id     INTEGER      NOT NULL,
    territory_name   VARCHAR(50)  NOT NULL,
    territory_group  VARCHAR(50)
);

CREATE TABLE public.customer (
    customer_id      INTEGER      NOT NULL,
    first_name       VARCHAR(100) NOT NULL,
    last_name        VARCHAR(100) NOT NULL,
    email_address    VARCHAR(200),
    customer_number  VARCHAR(20)
);
