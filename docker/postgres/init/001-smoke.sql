CREATE DATABASE evo1_oltp;
CREATE DATABASE evo1_olap;

\connect evo1_oltp

CREATE TABLE IF NOT EXISTS vat_smoke_test_reference (
    vat_id BIGSERIAL PRIMARY KEY,
    canton_code TEXT NOT NULL,
    category TEXT NOT NULL,
    effective_from DATE NOT NULL DEFAULT CURRENT_DATE
);

INSERT INTO vat_smoke_test_reference (canton_code, category)
VALUES
    ('ZH', 'standard'),
    ('BE', 'reduced'),
    ('GE', 'exempt')
ON CONFLICT DO NOTHING;

\connect evo1_olap

CREATE TABLE IF NOT EXISTS vat_smoke_test_sink (
    vat_id BIGINT PRIMARY KEY,
    canton_code TEXT NOT NULL,
    category TEXT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
