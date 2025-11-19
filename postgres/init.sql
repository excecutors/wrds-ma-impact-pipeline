CREATE SCHEMA IF NOT EXISTS bronze; -- 原始数据
CREATE SCHEMA IF NOT EXISTS silver; -- 清洗、转换后的数据
CREATE SCHEMA IF NOT EXISTS gold;   -- 最终分析聚合的数据

--- === BRONZE TABLES (Empty) ===

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
    percentacquired DECIMAL(10, 4),
    lastupdated DATE
);

CREATE TABLE IF NOT EXISTS bronze.ot_glb_companybuysiderelation (
    companyid BIGINT,
    ticker VARCHAR(50),
    targetcompanyid BIGINT,
    targetcompanyname VARCHAR(255),
    dealdate DATE,
    dealtype VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS bronze.ot_glb_company (
    companyid BIGINT,
    companyname VARCHAR(255),
    companylegalname VARCHAR(255),
    cikcode VARCHAR(20),
    businessstatus VARCHAR(100),
    ownershipstatus VARCHAR(100),
    companyfinancingstatus VARCHAR(100),
    universe VARCHAR(50),
    exchange VARCHAR,
    ticker VARCHAR(50),
    primaryindustrysector VARCHAR(100),
    primaryindustrygroup VARCHAR(100),
    primaryindustrycode VARCHAR(100),
    hqcountry VARCHAR(100),
    hqglobalsubregion VARCHAR(100),
    lastupdated DATE
);

CREATE TABLE IF NOT EXISTS bronze.ot_glb_companyindustryrelation (
    companyid BIGINT,
    industrysector VARCHAR(100),
    industrygroup VARCHAR(100),
    industrycode VARCHAR(100),
    isprimary VARCHAR(10)
    lastupdated DATE
);

CREATE TABLE IF NOT EXISTS bronze.ccmxpf_lnkhist (
    gvkey VARCHAR(20),
    linkprim VARCHAR(10),
    lpermco BIGINT,
    linkdt DATE,
    linkenddt DATE
);

CREATE TABLE IF NOT EXISTS bronze.fundq (
    gvkey VARCHAR(20),
    cik VARCHAR(20),
    rdq DATE,
    apdedateq DATE,
    dlttq DECIMAL(20, 4),
    dlcq DECIMAL(20, 4),
    cheq DECIMAL(20, 4),
    oibdpq DECIMAL(20, 4) -- EBITDA 
);

CREATE TABLE IF NOT EXISTS bronze.dsf (
    permco BIGINT,
    date DATE,
    prc DECIMAL(20, 4),
    shrout DECIMAL(20, 4)
);

GRANT ALL PRIVILEGES ON SCHEMA bronze TO admin;
GRANT ALL PRIVILEGES ON SCHEMA silver TO admin;
GRANT ALL PRIVILEGES ON SCHEMA gold TO admin;