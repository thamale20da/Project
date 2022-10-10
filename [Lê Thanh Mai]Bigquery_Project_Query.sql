-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) AS month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE date BETWEEN '20170101' AND '20170331'
GROUP BY 1
ORDER BY 1;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
  trafficSource.source,
  SUM(totals.visits) AS total_visits,
  SUM(totals.bounces) AS total_no_of_bounces, 
  SUM(totals.bounces)*100/SUM(totals.visits) AS bounce_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY 1
ORDER BY 2 DESC;


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
  (SELECT 'Month' AS time_type,
          FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS time,
          trafficSource.source,
          SUM(totals.totalTransactionRevenue)/1000000 AS revenue
  FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  GROUP BY 2,3)
  UNION ALL
  (SELECT 'Week' AS time_type,
          FORMAT_DATE('%Y%W', PARSE_DATE('%Y%m%d', date)) AS time,
          trafficSource.source, 
          SUM(totals.totalTransactionRevenue)/1000000 AS revenue
  FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  GROUP BY 2,3)
  ORDER BY 3,1,2; 


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH 
purchaser AS(
  SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) AS month,
  	 SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) AS avg_pageviews_purchase
  FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE totals.transactions IS NOT NULL AND date BETWEEN '20170601' AND '20170731'
  GROUP BY 1),

non_purchaser AS(
  SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) AS month,
  	 SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) AS avg_pageviews_non_purchase
  FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE totals.transactions IS NULL AND date BETWEEN '20170601' AND '20170731'
  GROUP BY 1)

SELECT p.*
       np.avg_pageviews_non_purchase 
FROM purchaser p
JOIN non_purchaser np ON p.month = np.month
ORDER BY 1;


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
  SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) AS month,
         SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) AS Avg_total_transactions_per_user
  FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  WHERE totals.transactions IS NOT NULL 
  GROUP BY 1;


-- Query 06: Average amount of money spent per session
#standardSQL
  SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) AS month,
         SUM(totals.totalTransactionRevenue)/COUNT(DISTINCT visitId) AS avg_revenue_by_user_per_visit 
  FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  WHERE totals.transactions IS NOT NULL
  GROUP BY 1;
  

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
  SELECT v2ProductName AS other_purchased_products,
         SUM(productQuantity) AS quantity
  FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, 
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product 
  WHERE fullVisitorId IN(                      
                          SELECT fullVisitorId   
                          FROM`bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, 
                          UNNEST(hits) AS hits,
                          UNNEST(hits.product) AS product 
                          WHERE productRevenue IS NOT NULL AND v2ProductName = "YouTube Men's Vintage Henley"
                          )
  AND v2ProductName != "YouTube Men's Vintage Henley" AND productRevenue IS NOT NULL
  GROUP BY 1
  ORDER BY 2 DESC;


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH                 
  view AS(                       
    SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) AS month,
           COUNT(hits.eCommerceAction.action_type) AS num_product_view
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, 
    UNNEST(hits) AS hits
    WHERE (date BETWEEN '20170101' AND '20170331')
    AND hits.eCommerceAction.action_type = '2'
    GROUP BY 1),
   
  cart AS(
    SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) AS month,
           COUNT(hits.eCommerceAction.action_type) AS num_addtocart
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, 
    UNNEST(hits) AS hits
    WHERE (date BETWEEN '20170101' AND '20170331') AND hits.eCommerceAction.action_type = '3'
    GROUP BY 1),
  
  purchase AS(
    SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) AS month,
           COUNT(hits.eCommerceAction.action_type) AS num_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, 
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
    WHERE (date BETWEEN '20170101' AND '20170331') AND hits.eCommerceAction.action_type = '6'
    GROUP BY 1)

SELECT 
      view.month,
      view.num_product_view,
      cart.num_addtocart,
      purchase.num_purchase, 
      ROUND((cart.num_addtocart*100/view.num_product_view),2) AS add_to_cart_rate, 
      ROUND((purchase.num_purchase*100/view.num_product_view),2) AS purchase_rate
FROM view
JOIN cart ON cart.month = view.month
JOIN purchase ON purchase.month = view.month
ORDER BY 1;