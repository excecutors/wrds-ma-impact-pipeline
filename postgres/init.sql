-- Create admin user if not exists (handled by POSTGRES_USER env var)
-- The user 'admin' will be created automatically by PostgreSQL from env variables

CREATE SCHEMA IF NOT EXISTS bronze; -- raw data ingested from external sources; only applied basic filtering
CREATE SCHEMA IF NOT EXISTS silver; -- cleaned and transformed data
CREATE SCHEMA IF NOT EXISTS gold;   -- final analysis results

--- === BRONZE TABLES ===

CREATE TABLE IF NOT EXISTS bronze.ot_glb_deal (
    dealid BIGINT,
    companyid BIGINT,
    companyname VARCHAR(255),
    dealdate DATE,
    announceddate DATE,
    dealsize DECIMAL(20, 2),
    nativecurrencyofdeal VARCHAR(50),
    dealstatus VARCHAR(100),
    dealtype VARCHAR(100),
    dealclass VARCHAR(100),
    percentacquired DECIMAL(10, 4)
);

CREATE TABLE IF NOT EXISTS bronze.ot_glb_company (
    companyid BIGINT,
    companyname VARCHAR(255),
    ticker VARCHAR(50),
    businessstatus VARCHAR(100),
    ownershipstatus VARCHAR(100),
    companyfinancingstatus VARCHAR(100),
    universe VARCHAR(50),
    hqglobalsubregion VARCHAR(100),
    hqcountry VARCHAR(100),
    exchange VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.ot_glb_companybuysiderelation (
    companyid BIGINT,
    targetcompanyid BIGINT,
    targetcompanyname VARCHAR(255),
    dealdate DATE,
    dealtype VARCHAR(100)
);


CREATE TABLE IF NOT EXISTS bronze.company_industry_relation (
    ticker VARCHAR(50),
    primaryindustrysector VARCHAR(100)
);



CREATE TABLE IF NOT EXISTS bronze.fundq (
    gvkey VARCHAR(20),
    fyearq INT,
    fqtr INT,
    apdedateq DATE, -- Actual Period End Date
    tic VARCHAR(50),
    curncdq VARCHAR(10),
    dlttq DECIMAL(20, 4),
    dlcq DECIMAL(20, 4),
    cheq DECIMAL(20, 4),
    prccq DECIMAL(20, 4), -- Price Close - Quarter
    prchq DECIMAL(20, 4), -- Price High - Quarter
    prclq DECIMAL(20, 4), -- Price Low - Quarter
    cshoq DECIMAL(20, 4), -- Common Shares Outstanding
    oibdpq DECIMAL(20, 4) -- EBITDA 
);



GRANT ALL PRIVILEGES ON SCHEMA bronze TO admin;
GRANT ALL PRIVILEGES ON SCHEMA silver TO admin;
GRANT ALL PRIVILEGES ON SCHEMA gold TO admin;