# Databricks notebook source
# MAGIC %md
# MAGIC ## Data access
# MAGIC For the purpose of that demo, we will be using free sample data from databricks marketplace. 

# COMMAND ----------

# Reading from 2 public shares on marketplace
db_catalog_news = 'fsgtm_market_news'
db_catalog_market = 'fsgtm_market_data'

# Writing to a dedicated catalog
genie_catalog = 'fsgtm'
genie_schema = 'genie_cap_markets'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create data model

# COMMAND ----------

_ = sql(f'CREATE CATALOG IF NOT EXISTS {genie_catalog}')
_ = sql(f'CREATE DATABASE IF NOT EXISTS {genie_catalog}.{genie_schema}')

# COMMAND ----------

_ = sql(f'''CREATE OR REPLACE TABLE {genie_catalog}.{genie_schema}.portfolio (   
  `ticker`              STRING COMMENT 'Unique identifier for the stock, allowing easy reference and tracking.',
  `company_name`        STRING COMMENT 'The name of the company, providing a recognizable and human-readable label for identification.',
  `company_description` STRING COMMENT 'A brief overview of the company, its products, and services, generated using DBRX model.',
  `company_website`     STRING COMMENT 'The official website of the company, providing more detailed information and resources.',
  `company_logo`        STRING COMMENT 'The visual representation of the company, allowing for easy recognition and branding.',
  `industry`            STRING COMMENT 'The industry or sector in which the company operates, providing context for the company business and potential competitors.
'
) USING DELTA
COMMENT 'The `portfolio` table contains information about the companies in our investment portfolio. It includes details about the company ticker, name, description, website, logo, and industry. This data can be used to analyze the portfolio composition, monitor industry trends, and perform research on individual companies. It can also be used to generate reports and visualizations for stakeholders, such as the portfolio diversity and the performance of companies in specific industries.'
''')

# COMMAND ----------

_ = sql(f'''CREATE OR REPLACE TABLE {genie_catalog}.{genie_schema}.fundamentals (   
  `ticker`                STRING  COMMENT 'Unique identifier for the stock, allowing easy reference and tracking.',
  `market_capitalization` DOUBLE  COMMENT 'Represents the current market capitalization of the stock, indicating the stock value in the market.',
  `outstanding_shares`    DOUBLE  COMMENT 'Represents the number of outstanding shares of the stock, indicating the liquidity and ownership of the stock.'
) USING DELTA
COMMENT 'The `fundamentals` table contains fundamental information about various stocks, including market capitalization and outstanding shares. This data can be used to analyze the financial health of individual stocks, as well as to compare the performance of different stocks over time. It can also be used to identify trends in market capitalization and outstanding shares, which can inform investment decisions and market analysis.'
''')

# COMMAND ----------

_ = sql(f'''CREATE OR REPLACE TABLE {genie_catalog}.{genie_schema}.prices (   
  `ticker`          STRING COMMENT 'Unique identifier for the stock or security, allowing easy reference and tracking.',
  `date`            DATE COMMENT 'The date for which the price information is provided.',
  `open`            DOUBLE COMMENT '
Represents the opening price of the security on the given date.',
  `high`            DOUBLE COMMENT 'Represents the highest price of the security on the given date.',
  `low`             DOUBLE COMMENT 'Represents the lowest price of the security on the given date.',
  `close`           DOUBLE COMMENT 'Represents the closing price of the security on the given date.',
  `adjusted_close`  DOUBLE COMMENT 'Represents the adjusted closing price of the security on the given date, accounting for any corporate actions or other adjustments. This represents the cash value of the last transacted price before the market closes',
  `return`          DOUBLE COMMENT 'Represents the return of the security on the given date, calculated as the difference between the closing price and last day closing price.',
  `volume`          DOUBLE COMMENT 'Represents the trading volume of the security on the given date, indicating the number of shares traded.',
  `split_factor`    DOUBLE COMMENT 'Represent the stock split of a given ticker at any point in time'
) USING DELTA
COMMENT 'The `prices` table contains stock price data for various tickers. It includes information on daily open, high, low, and closing prices, as well as adjusted closing prices, returns, and trading volumes. This data can be used for stock analysis, trend identification, and risk assessment. It can also be used to generate reports and visualizations for stakeholders to monitor market trends and make informed decisions.'
''')

# COMMAND ----------

_ = sql(f'''CREATE OR REPLACE TABLE {genie_catalog}.{genie_schema}.news_ticker (   
  `ticker`     STRING COMMENT 'Unique identifier for the stock ticker, allowing easy reference and tracking of specific stocks.',
  `article_id` STRING COMMENT 'Identifier for the news article related to the stock ticker, enabling linking articles to their respective tickers.'
) USING DELTA
COMMENT 'The `news_ticker` table contains information about ticker symbols and the corresponding news articles. It can be used to track news related to various ticker symbols, enabling users to monitor market trends and news that may impact the performance of their investments. This table can also be used to identify ticker symbols associated with specific news articles, making it easier to analyze the impact of news on financial markets.'
''')

# COMMAND ----------

_ = sql(f'''CREATE OR REPLACE TABLE {genie_catalog}.{genie_schema}.news (   
  `article_id`        STRING     COMMENT 'Unique identifier for each news article.',
  `published_time`    TIMESTAMP  COMMENT 'The time when the article was published.',
  `source`            STRING     COMMENT 'The news source or publisher that published the article.',
  `source_url`        STRING     COMMENT 'The URL of the article, allowing users to access the original content.',
  `title`             STRING     COMMENT 'The title of the news article, providing a brief overview of the content.',
  `sentiment`         DOUBLE     COMMENT 'Represents the sentiment or tone of the article, measured as a double value between -1 (negative) and 1 (positive).',
  `market_sentiment`  STRING     COMMENT 'Represents the market sentiment for a given article, can be Bearish, Bullish, or Neutral'
) USING DELTA
COMMENT 'The `news` table contains articles from various sources related to the financial markets. It includes details such as the article title, the source, and the sentiment of the article. This data can be used to monitor market trends, track sentiment changes, and analyze the impact of different news sources on market behavior. This information can be particularly useful for traders and analysts who need to stay up-to-date with market news and understand how it might affect their investments.'
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest data

# COMMAND ----------

insert = sql(f'''INSERT INTO {genie_catalog}.{genie_schema}.portfolio
SELECT 
  ticker,
  companyName AS company_name,
  ai_query(
    'databricks-dbrx-instruct',
    concat_ws(' ', 'Describe company', companyName, 'in the', industry, 'industry')
  ) AS company_description,
  website AS company_website,
  logo AS company_logo,
  industry
FROM 
  {db_catalog_market}.market_data.company_profile
WHERE 
  ticker IS NOT NULL
  AND ticker != 'NaN'
  AND companyName IS NOT NULL''')

display(insert)

# COMMAND ----------

insert = sql(f'''INSERT INTO {genie_catalog}.{genie_schema}.fundamentals
SELECT 
  ticker,
  marketCap AS market_capitalization,
  sharesOutstanding AS outstanding_shares
FROM 
  {db_catalog_market}.market_data.company_profile
WHERE 
  ticker IS NOT NULL
  AND ticker != 'NaN'
  AND companyName IS NOT NULL''')

display(insert)

# COMMAND ----------

insert = sql(f'''INSERT INTO {genie_catalog}.{genie_schema}.prices
SELECT 
  ticker,
  `date`,
  `open`,
  `high`,
  `low`,
  `close`,
  `adjClose` AS `adjusted_close`,
  -- compute investment return as a window function
  adjClose/(lag(adjClose) OVER (PARTITION BY ticker ORDER BY `date`))-1 AS `return`,
  `vol` AS `volume`,
  `splitFactor` AS `split_factor`
FROM 
  {db_catalog_market}.market_data.dailyprice
WHERE 
  ticker IS NOT NULL''')

display(insert)

# COMMAND ----------

insert = sql(f'''INSERT INTO {genie_catalog}.{genie_schema}.prices
SELECT 
  ticker,
  `date`,
  `open`,
  `high`,
  `low`,
  `close`,
  `adjClose` AS `adjusted_close`, -- the cash value of the last transacted price before the market closes
  `vol` AS `volume`,
  `splitFactor` AS `split`
FROM 
  {db_catalog_market}.market_data.dailyprice
WHERE 
  ticker IS NOT NULL''')

display(insert)

# COMMAND ----------

insert = sql(f'''INSERT INTO {genie_catalog}.{genie_schema}.news_ticker
SELECT 
  ticker,
  articleId AS article_id 
FROM 
  {db_catalog_news}.market_data.news_ticker_sentiment''')

display(insert)

# COMMAND ----------

insert = sql(f'''INSERT INTO {genie_catalog}.{genie_schema}.news
SELECT DISTINCT
  articleId AS article_id,
  publishedTime AS published_time,
  source,
  `url` AS source_url,
  title AS title,
  articleSentimentScore AS sentiment,
  articleSentimentLabel AS market_sentiment  
FROM 
  {db_catalog_news}.market_data.news''')

display(insert)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Room

# COMMAND ----------

# MAGIC %md
# MAGIC Here are some example questions that could be answered using the provided tables and their data:
# MAGIC
# MAGIC - What are the top 5 companies by market capitalization in our investment portfolio?
# MAGIC - What is the average sentiment of news articles related to a particular stock (e.g., ticker 'GOOGL')?
# MAGIC - Can you show me a bar chart of the number of news articles published per day?