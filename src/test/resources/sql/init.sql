CREATE TABLE PUBLIC.TRANSACTIONS
(
    TRANSACTION_ID  VARCHAR(50) NOT NULL,
    GUARANTORS      VARCHAR(500) NOT NULL,
    SENDER_NAME     VARCHAR(50) NOT NULL,
    RECEIVER_NAME   VARCHAR(50) NOT NULL,
    TXN_TYPE        VARCHAR(50) NOT NULL,
    COUNTRY         VARCHAR(50) NOT NULL,
    CURRENCY        VARCHAR(50) NOT NULL,
    AMOUNT          DECIMAL NOT NULL,
    STATUS          VARCHAR(50) NOT NULL,
    INITIATION_DATE DATE NOT NULL,
    PARTITION_DATE  VARCHAR(50) NOT NULL
) PARTITION BY LIST (PARTITION_DATE);

CREATE TABLE TRANSACTIONS_2025_01_25 PARTITION OF TRANSACTIONS FOR VALUES IN ('2025-01-25');

INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_001', '[{"NAME": "G_001", "PERCENTAGE": 0.6}]', 'COMP_1', 'COMP_2', 'CLASS_A', 'ES', 'EUR', 1000, 'OPEN', '2025-01-01', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_001', '[{"NAME": "G_001", "PERCENTAGE": 0.6}]', 'COMP_1', 'COMP_2', 'CLASS_A', 'ES', 'EUR', 1000, 'OPEN', '2025-01-01', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_002', '[{"NAME": "G_002", "PERCENTAGE": 0.5}]', 'COMP_2', 'COMP_3', 'CLASS_B', 'ES', 'EUR', 2000, 'OPEN', '2025-01-03', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_009', '[{"NAME": "G_002", "PERCENTAGE": 0.5}]', 'COMP_4', 'COMP_3', 'CLASS_B', 'ES', 'EUR', 9000, 'OPEN', '2022-01-03', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_003', '[{"NAME": "G_001", "PERCENTAGE": 0.5}, {"NAME": "G_003", "PERCENTAGE": 0.5}]', 'COMP_1', 'COMP_4', 'CLASS_C', 'ES', 'EUR', 3000, 'CLOSED', '2025-01-05', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_004', '[{"NAME": "G_005", "PERCENTAGE": 0.5}]', 'COMP_5', 'COMP_6', 'CLASS_A', 'US', 'USD', 4000, 'OPEN', '2025-01-07', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_005', '[{"NAME": "G_005", "PERCENTAGE": 0.6}]', 'COMP_5', 'COMP_7', 'CLASS_D', 'US', 'USD', 5000, 'OPEN', '2025-01-09', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_006', '[{"NAME": "G_007", "PERCENTAGE": 0.5}]', 'COMP_5', 'COMP_8', 'CLASS_B', 'US', 'USD', 6000, 'OPEN', '2025-01-11', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_007', '[{"NAME": "G_006", "PERCENTAGE": 0.2}, {"NAME": "G_007", "PERCENTAGE": 0.2}]', 'COMP_6', 'COMP_7', 'CLASS_A', 'US', 'USD', 7000, 'OPEN', '2025-01-13', '2025-01-25');
INSERT INTO public.transactions (transaction_id, guarantors, sender_name, receiver_name, txn_type, country, currency, amount, status, initiation_date, partition_date) VALUES ('T_008', '[{"NAME": "G_001", "PERCENTAGE": 0.5}, {"NAME": "G_004", "PERCENTAGE": 0.5}]', 'COMP_3', 'COMP_2', 'CLASS_B', 'ES', 'EUR', 8000, 'OPEN', '2025-01-15', '2025-01-25');

CREATE TABLE PUBLIC.RECEIVERS_REGISTRY
(
    RECEIVER_ACCOUNT VARCHAR(50) NOT NULL,
    COUNTRY          VARCHAR(50) NOT NULL,
    PARTITION_DATE   VARCHAR(50) NOT NULL
) PARTITION BY LIST (PARTITION_DATE);

CREATE TABLE RECEIVERS_REGISTRY_2025_01_15 PARTITION OF RECEIVERS_REGISTRY FOR VALUES IN ('2025-01-15');
CREATE TABLE RECEIVERS_REGISTRY_2024_08_01 PARTITION OF RECEIVERS_REGISTRY FOR VALUES IN ('2024-08-01');

INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_1', 'ES', '2025-01-15');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_2', 'ES', '2025-01-15');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_3', 'ES', '2025-01-15');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_4', 'ES', '2025-01-15');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_5', 'US', '2025-01-15');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_6', 'US', '2025-01-15');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_7', 'US', '2025-01-15');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_1', 'ES', '2024-08-01');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_2', 'ES', '2024-08-01');
INSERT INTO public.receivers_registry (receiver_account, country, partition_date) VALUES ('COMP_7', 'US', '2024-08-01');

CREATE TABLE PUBLIC.EXCHANGE_RATES
(
    FROM_CURRENCY  VARCHAR(50) NOT NULL,
    TO_CURRENCY    VARCHAR(50) NOT NULL,
    RATE           DECIMAL NOT NULL,
    PARTITION_DATE VARCHAR(50) NOT NULL
) PARTITION BY LIST (PARTITION_DATE);

CREATE TABLE EXCHANGE_RATES_2025_01_20 PARTITION OF EXCHANGE_RATES FOR VALUES IN ('2025-01-20');
CREATE TABLE EXCHANGE_RATES_2025_01_01 PARTITION OF EXCHANGE_RATES FOR VALUES IN ('2025-01-01');

INSERT INTO public.exchange_rates (from_currency, to_currency, rate, partition_date) VALUES ('EUR', 'EUR', 1, '2025-01-20');
INSERT INTO public.exchange_rates (from_currency, to_currency, rate, partition_date) VALUES ('USD', 'EUR', 0.9, '2025-01-20');
INSERT INTO public.exchange_rates (from_currency, to_currency, rate, partition_date) VALUES ('GBP', 'EUR', 1.2, '2025-01-20');
INSERT INTO public.exchange_rates (from_currency, to_currency, rate, partition_date) VALUES ('EUR', 'EUR', 1, '2025-01-01');
INSERT INTO public.exchange_rates (from_currency, to_currency, rate, partition_date) VALUES ('USD', 'EUR', 0.95, '2025-01-01');
INSERT INTO public.exchange_rates (from_currency, to_currency, rate, partition_date) VALUES ('GBP', 'EUR', 1.25, '2025-01-01');
