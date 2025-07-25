CREATE DATABASE reddit_trends_project;
GO

/* ============================================================
    Project : Reddit Trends – SQL Analysis
    Database: reddit_trends_project
    Author  : Maryna
    Date    : 2025-07-22
    Desc    : Reproduce EDA insights using T-SQL
=============================================================== */

USE reddit_trends_project;
GO

/* ============================================================
   00. CLEANUP (run if re-importing)
=============================================================== */
IF OBJECT_ID('dbo.STG_Reddit', 'U') IS NOT NULL DROP TABLE dbo.STG_Reddit;
IF OBJECT_ID('dbo.RAW_Reddit', 'U') IS NOT NULL DROP TABLE dbo.RAW_Reddit;
IF OBJECT_ID('dbo.WRK_Reddit', 'U') IS NOT NULL DROP TABLE dbo.WRK_Reddit;
IF OBJECT_ID('dbo.TOKENS_Reddit', 'U') IS NOT NULL DROP TABLE dbo.TOKENS_Reddit;

IF OBJECT_ID('dbo.VW_Reddit_Base', 'V') IS NOT NULL DROP VIEW dbo.VW_Reddit_Base;
IF OBJECT_ID('dbo.VW_Reddit_MediansByHour', 'V') IS NOT NULL DROP VIEW dbo.VW_Reddit_MediansByHour;

IF OBJECT_ID('dbo.RES_TopWords', 'U') IS NOT NULL DROP TABLE dbo.RES_TopWords;
GO

/* ============================================================
   01. STAGING TABLE: flat load of the CSV
=============================================================== */
CREATE TABLE dbo.STG_Reddit (
    clean_title   VARCHAR(4000),
    score         VARCHAR(50),
    num_comments  VARCHAR(50),
    created_utc   VARCHAR(50),
    subreddit     VARCHAR(100),
    created_date  VARCHAR(50),
    year_month    VARCHAR(50),
    tokens        VARCHAR(MAX),
    tokens_json   VARCHAR(MAX)
);

/* ============================================================
   02. BULK INSERT STG
=============================================================== */
BULK INSERT dbo.STG_Reddit
FROM '/var/opt/mssql/data/cleaned_reddit_posts_pipe.csv' 
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = '|',
    ROWTERMINATOR   = '\n',
    KEEPNULLS,
    ERRORFILE       = '/var/opt/mssql/data/bulk_err_no_title.log',
    MAXERRORS       = 2000,
    TABLOCK
);
GO

/* Quick check */
    SELECT TOP 10 * FROM dbo.STG_Reddit;
    SELECT COUNT(*) AS loaded_rows FROM dbo.STG_Reddit;

/* ============================================================
   03. RAW TABLE
=============================================================== */
CREATE TABLE dbo.RAW_Reddit (
    RowId           INT IDENTITY(1,1) PRIMARY KEY,
    clean_title     NVARCHAR(4000),
    score           INT,
    num_comments    INT,
    created_utc     BIGINT,
    subreddit       VARCHAR(100),
    created_date    DATETIME2(0),
    year_month      CHAR(7),       -- 'YYYY-MM'
    tokens          NVARCHAR(MAX),
    tokens_json     NVARCHAR(MAX)
);
GO

/* ============================================================
   04. INSERT RAW
=============================================================== */
INSERT INTO dbo.RAW_Reddit (
    clean_title, score, num_comments, created_utc,
    subreddit, created_date, year_month, tokens, tokens_json
)
SELECT
    clean_title,
    TRY_CONVERT(INT, REPLACE(score, '"', '')),
    TRY_CONVERT(INT, REPLACE(num_comments, '"', '')),
    TRY_CONVERT(BIGINT, CAST(REPLACE(created_utc, '"', '') AS FLOAT)),
    subreddit,
    TRY_CONVERT(DATETIME2(0), REPLACE(created_date, '"', ''), 120),
    LEFT(REPLACE(year_month, '"', ''), 7),
    tokens,
    tokens_json
FROM dbo.STG_Reddit;
GO

/* Quick check */
SELECT COUNT(*) AS typed_rows FROM dbo.RAW_Reddit;
SELECT TOP 5 * FROM dbo.RAW_Reddit;

/* ============================================================
   05. WRK TABLE – derive extra fields (year, month, hour_utc)
=============================================================== */
CREATE TABLE dbo.WRK_Reddit (
    RowId               INT PRIMARY KEY,          
    subreddit           VARCHAR(100) NOT NULL,
    clean_title         VARCHAR(4000) NOT NULL,
    score               INT NOT NULL,
    num_comments        INT NOT NULL,
    created_utc         BIGINT NOT NULL,
    created_date        DATETIME2(0) NOT NULL,
    year                INT NOT NULL,
    month               INT NOT NULL,
    year_month          CHAR(7) NOT NULL,
    hour_utc            INT NOT NULL
    
);
GO

INSERT INTO dbo.WRK_Reddit (
    RowId,
    subreddit,
    clean_title,
    score,
    num_comments,
    created_utc,
    created_date,
    year,
    month,
    year_month,
    hour_utc
)
SELECT
    RowId,
    subreddit,
    clean_title,
    score,
    num_comments,
    created_utc,
    created_date,
    DATEPART(YEAR,  created_date) AS year,
    DATEPART(MONTH, created_date) AS month,
    year_month,
    DATEPART(HOUR, created_date)  AS hour_utc
FROM dbo.RAW_Reddit;
GO

/* Quick check */
SELECT TOP 5 * FROM dbo.WRK_Reddit ORDER BY RowId;
SELECT COUNT(*) AS wrk_rows FROM dbo.WRK_Reddit;

/* ============================================================
   06. TOKENS TABLE
=============================================================== */
CREATE TABLE dbo.TOKENS_Reddit
(
    TokenId INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    RowId   INT NOT NULL,
    token   NVARCHAR(200) NOT NULL
);
GO

ALTER TABLE dbo.TOKENS_Reddit
  ADD CONSTRAINT FK_TOKENS_Reddit_Raw
  FOREIGN KEY (RowId) REFERENCES dbo.RAW_Reddit(RowId);

CREATE INDEX IX_TOKENS_Reddit_RowId ON dbo.TOKENS_Reddit (RowId);
CREATE INDEX IX_TOKENS_Reddit_Token ON dbo.TOKENS_Reddit (token);
GO

/* Insert tokens from JSON array */
INSERT INTO dbo.TOKENS_Reddit (RowId, token)
SELECT  r.RowId,
        j.value AS token
FROM    dbo.RAW_Reddit AS r
CROSS APPLY OPENJSON(r.tokens_json) AS j
WHERE   r.tokens_json IS NOT NULL;
GO

/* Quick check */
SELECT TOP 100 * FROM dbo.TOKENS_Reddit;
SELECT COUNT(*) AS total_tokens FROM dbo.TOKENS_Reddit;
GO

/* ============================================================
   07. BASE VIEW – convenient subset
=============================================================== */
CREATE OR ALTER VIEW dbo.VW_Reddit_Base
AS
SELECT  RowId,
        subreddit,
        clean_title,
        score,
        num_comments,
        created_date,
        year,
        month,
        hour_utc,
        year_month
FROM dbo.WRK_Reddit;
GO

/* ============================================================
   08. INSIGHT QUERIES – replicate EDA
=============================================================== */

-- 8.1 Posts per year by subreddit
SELECT  year,
        subreddit,
        COUNT(*) AS posts_count
FROM    dbo.VW_Reddit_Base
GROUP BY year, subreddit
ORDER BY year, subreddit;

-- 8.2 Average score & comments per year by subreddit
SELECT  year,
        subreddit,
        AVG(score)        AS avg_score,
        AVG(num_comments) AS avg_comments
FROM    dbo.VW_Reddit_Base
GROUP BY year, subreddit
ORDER BY year, subreddit;
GO

-- 8.3 Median score per hour (UTC) by subreddit
CREATE OR ALTER VIEW dbo.VW_Reddit_MediansByHour
AS
SELECT DISTINCT
       subreddit,
       hour_utc,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score)
       OVER (PARTITION BY subreddit, hour_utc) AS median_score
FROM dbo.VW_Reddit_Base;
GO

SELECT * 
FROM dbo.VW_Reddit_MediansByHour
ORDER BY subreddit, hour_utc;

-- Pivot median scores by hour
SELECT subreddit, [0] AS h0, [1] AS h1, [2] AS h2, [3] AS h3, [4] AS h4, [5] AS h5,
       [6] AS h6, [7] AS h7, [8] AS h8, [9] AS h9, [10] AS h10, [11] AS h11,
       [12] AS h12, [13] AS h13, [14] AS h14, [15] AS h15, [16] AS h16, [17] AS h17,
       [18] AS h18, [19] AS h19, [20] AS h20, [21] AS h21, [22] AS h22, [23] AS h23
FROM (
    SELECT subreddit, hour_utc,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score)
           OVER (PARTITION BY subreddit, hour_utc) AS med
    FROM dbo.VW_Reddit_Base
) src
PIVOT (
    MAX(med) FOR hour_utc IN
    ([0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],
     [12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23])
) pvt
ORDER BY subreddit;
GO

-- 8.4 Top-15 words per subreddit
;WITH freq AS (
    SELECT w.subreddit, t.token, COUNT(*) AS freq
    FROM dbo.TOKENS_Reddit t
    JOIN dbo.WRK_Reddit  w ON w.RowId = t.RowId
    GROUP BY w.subreddit, t.token
)
SELECT subreddit, token, freq
FROM (
    SELECT subreddit, token, freq,
           ROW_NUMBER() OVER (PARTITION BY subreddit ORDER BY freq DESC) AS rn
    FROM freq
) x
WHERE rn <= 15
ORDER BY subreddit, freq DESC;

-- 8.5 Score vs comments buckets
SELECT subreddit,
       CASE 
           WHEN score < 10000   THEN 'low'
           WHEN score < 100000  THEN 'mid'
           ELSE 'high'
       END AS score_bucket,
       CASE 
           WHEN num_comments < 1000  THEN 'low'
           WHEN num_comments < 5000  THEN 'mid'
           ELSE 'high'
       END AS comments_bucket,
       COUNT(*) AS cnt
FROM dbo.VW_Reddit_Base
GROUP BY subreddit,
         CASE 
             WHEN score < 10000   THEN 'low'
             WHEN score < 100000  THEN 'mid'
             ELSE 'high'
         END,
         CASE 
             WHEN num_comments < 1000  THEN 'low'
             WHEN num_comments < 5000  THEN 'mid'
             ELSE 'high'
         END
ORDER BY subreddit, score_bucket, comments_bucket;
GO

/* ============================================================
   09. Save top words for export
=============================================================== */
CREATE TABLE dbo.RES_TopWords
(
    subreddit VARCHAR(100),
    token     VARCHAR(200),
    freq      INT
);
GO

INSERT INTO dbo.RES_TopWords(subreddit, token, freq)
SELECT subreddit, token, freq
FROM (
    SELECT w.subreddit, t.token, COUNT(*) AS freq,
           ROW_NUMBER() OVER (PARTITION BY w.subreddit ORDER BY COUNT(*) DESC) AS rn
    FROM dbo.TOKENS_Reddit t
    JOIN dbo.WRK_Reddit   w ON w.RowId = t.RowId
    GROUP BY w.subreddit, t.token
) x
WHERE rn <= 15;
GO

/* ============================================================
   END OF FILE
=============================================================== */