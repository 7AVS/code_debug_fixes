-- VVD Campaign Performance Analysis SQL Pipeline
-- Analyzes 18 months of campaign data for ~27M deployments and ~4M clients

-- Set date parameters
SET @analysis_end_date = CURRENT_DATE();
SET @analysis_start_date = DATEADD(MONTH, -18, @analysis_end_date);

-- 1. Create base view with contact frequency calculations
CREATE OR REPLACE VIEW vvd_contact_frequency AS
WITH contact_history AS (
    SELECT 
        TACTIC_ID,
        CLIENT_ID,
        CAMPAIGN_ID,
        CAMPAIGN_NAME,
        DEPLOYMENT_DATE,
        CHANNEL,
        OFFER_TYPE,
        RESPONSE_FLAG,
        RESPONSE_DATE,
        CONVERSION_FLAG,
        CONVERSION_DATE,
        REVENUE,
        
        -- Calculate days since last contact
        DATEDIFF(DAY, 
            LAG(DEPLOYMENT_DATE) OVER (PARTITION BY CLIENT_ID ORDER BY DEPLOYMENT_DATE),
            DEPLOYMENT_DATE
        ) AS days_since_last_contact,
        
        -- Contact sequence number
        ROW_NUMBER() OVER (PARTITION BY CLIENT_ID ORDER BY DEPLOYMENT_DATE) AS contact_number,
        
        -- Contacts in rolling windows
        COUNT(*) OVER (
            PARTITION BY CLIENT_ID 
            ORDER BY DEPLOYMENT_DATE 
            RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW
        ) - 1 AS contacts_last_30d,
        
        COUNT(*) OVER (
            PARTITION BY CLIENT_ID 
            ORDER BY DEPLOYMENT_DATE 
            RANGE BETWEEN INTERVAL '60' DAY PRECEDING AND CURRENT ROW
        ) - 1 AS contacts_last_60d,
        
        COUNT(*) OVER (
            PARTITION BY CLIENT_ID 
            ORDER BY DEPLOYMENT_DATE 
            RANGE BETWEEN INTERVAL '90' DAY PRECEDING AND CURRENT ROW
        ) - 1 AS contacts_last_90d
        
    FROM vvd_campaign_deployments
    WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date
)
SELECT * FROM contact_history;

-- 2. Campaign Performance Summary
CREATE OR REPLACE VIEW vvd_campaign_performance AS
SELECT 
    CAMPAIGN_ID,
    CAMPAIGN_NAME,
    CHANNEL,
    OFFER_TYPE,
    
    -- Volume metrics
    COUNT(DISTINCT TACTIC_ID) AS total_deployments,
    COUNT(DISTINCT CLIENT_ID) AS unique_clients,
    
    -- Response metrics
    SUM(RESPONSE_FLAG) AS total_responses,
    SUM(CONVERSION_FLAG) AS total_conversions,
    SUM(REVENUE) AS total_revenue,
    
    -- Rate calculations
    ROUND(100.0 * SUM(RESPONSE_FLAG) / COUNT(*), 2) AS response_rate,
    ROUND(100.0 * SUM(CONVERSION_FLAG) / COUNT(*), 2) AS conversion_rate,
    ROUND(100.0 * SUM(CONVERSION_FLAG) / NULLIF(SUM(RESPONSE_FLAG), 0), 2) AS response_to_conversion_rate,
    
    -- Revenue metrics
    ROUND(AVG(REVENUE), 2) AS avg_revenue_per_deployment,
    ROUND(SUM(REVENUE) / NULLIF(COUNT(DISTINCT CLIENT_ID), 0), 2) AS revenue_per_client,
    
    -- Time to action metrics
    ROUND(AVG(
        CASE 
            WHEN RESPONSE_FLAG = 1 THEN DATEDIFF(DAY, DEPLOYMENT_DATE, RESPONSE_DATE)
            ELSE NULL 
        END
    ), 1) AS avg_days_to_response,
    
    ROUND(AVG(
        CASE 
            WHEN CONVERSION_FLAG = 1 THEN DATEDIFF(DAY, DEPLOYMENT_DATE, CONVERSION_DATE)
            ELSE NULL 
        END
    ), 1) AS avg_days_to_conversion
    
FROM vvd_campaign_deployments
WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date
GROUP BY CAMPAIGN_ID, CAMPAIGN_NAME, CHANNEL, OFFER_TYPE;

-- 3. Contact Frequency Impact Analysis
CREATE OR REPLACE VIEW vvd_frequency_impact AS
SELECT 
    contacts_last_30d,
    COUNT(*) AS deployment_count,
    COUNT(DISTINCT CLIENT_ID) AS unique_clients,
    
    -- Performance by frequency
    ROUND(100.0 * AVG(RESPONSE_FLAG), 2) AS avg_response_rate,
    ROUND(100.0 * AVG(CONVERSION_FLAG), 2) AS avg_conversion_rate,
    ROUND(AVG(REVENUE), 2) AS avg_revenue,
    ROUND(STDDEV(REVENUE), 2) AS revenue_stddev,
    
    -- Statistical confidence
    CASE 
        WHEN COUNT(*) >= 1000 THEN 'High'
        WHEN COUNT(*) >= 100 THEN 'Medium'
        ELSE 'Low'
    END AS confidence_level
    
FROM vvd_contact_frequency
GROUP BY contacts_last_30d
HAVING COUNT(*) >= 30  -- Minimum sample size
ORDER BY contacts_last_30d;

-- 4. Client Engagement Segmentation
CREATE OR REPLACE VIEW vvd_client_engagement AS
WITH client_metrics AS (
    SELECT 
        CLIENT_ID,
        COUNT(*) AS total_contacts,
        SUM(RESPONSE_FLAG) AS total_responses,
        SUM(CONVERSION_FLAG) AS total_conversions,
        SUM(REVENUE) AS total_revenue,
        MIN(DEPLOYMENT_DATE) AS first_contact_date,
        MAX(DEPLOYMENT_DATE) AS last_contact_date,
        
        -- Engagement rates
        ROUND(100.0 * SUM(RESPONSE_FLAG) / COUNT(*), 2) AS response_rate,
        ROUND(100.0 * SUM(CONVERSION_FLAG) / COUNT(*), 2) AS conversion_rate,
        ROUND(SUM(REVENUE) / COUNT(*), 2) AS revenue_per_contact
        
    FROM vvd_campaign_deployments
    WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date
    GROUP BY CLIENT_ID
)
SELECT 
    *,
    DATEDIFF(DAY, last_contact_date, CURRENT_DATE()) AS days_since_last_contact,
    DATEDIFF(DAY, first_contact_date, last_contact_date) AS customer_lifetime_days,
    
    -- Engagement score (weighted composite)
    ROUND(
        (response_rate * 0.3) + 
        (conversion_rate * 0.5) + 
        (LEAST(revenue_per_contact / 100, 1) * 0.2) * 100,
    2) AS engagement_score,
    
    -- Client segment
    CASE 
        WHEN conversion_rate >= 10 AND total_revenue >= 1000 THEN 'High Value'
        WHEN conversion_rate >= 5 OR total_revenue >= 500 THEN 'Medium Value'
        WHEN response_rate >= 20 THEN 'Engaged Non-Converter'
        WHEN total_contacts >= 10 THEN 'Over-Contacted'
        ELSE 'Low Engagement'
    END AS client_segment
    
FROM client_metrics;

-- 5. Optimal Contact Frequency by Channel
CREATE OR REPLACE VIEW vvd_optimal_frequency AS
SELECT 
    CHANNEL,
    contacts_last_30d,
    COUNT(*) AS sample_size,
    
    -- Performance metrics
    ROUND(100.0 * AVG(RESPONSE_FLAG), 2) AS response_rate,
    ROUND(100.0 * AVG(CONVERSION_FLAG), 2) AS conversion_rate,
    ROUND(AVG(REVENUE), 2) AS avg_revenue,
    
    -- Lift vs baseline (0 contacts in last 30d)
    ROUND(100.0 * (AVG(CONVERSION_FLAG) - 
        FIRST_VALUE(AVG(CONVERSION_FLAG)) OVER (PARTITION BY CHANNEL ORDER BY contacts_last_30d)
    ) / NULLIF(
        FIRST_VALUE(AVG(CONVERSION_FLAG)) OVER (PARTITION BY CHANNEL ORDER BY contacts_last_30d), 0
    ), 2) AS conversion_lift_pct,
    
    -- Rank by revenue within channel
    RANK() OVER (PARTITION BY CHANNEL ORDER BY AVG(REVENUE) DESC) AS revenue_rank
    
FROM vvd_contact_frequency
GROUP BY CHANNEL, contacts_last_30d
HAVING COUNT(*) >= 100  -- Statistical significance
ORDER BY CHANNEL, contacts_last_30d;

-- 6. Monthly Performance Trends
CREATE OR REPLACE VIEW vvd_monthly_trends AS
SELECT 
    DATE_FORMAT(DEPLOYMENT_DATE, '%Y-%m') AS year_month,
    CHANNEL,
    
    -- Volume metrics
    COUNT(*) AS deployments,
    COUNT(DISTINCT CLIENT_ID) AS unique_clients,
    
    -- Performance metrics
    ROUND(100.0 * SUM(RESPONSE_FLAG) / COUNT(*), 2) AS response_rate,
    ROUND(100.0 * SUM(CONVERSION_FLAG) / COUNT(*), 2) AS conversion_rate,
    SUM(REVENUE) AS total_revenue,
    
    -- Month-over-month changes
    LAG(COUNT(*)) OVER (PARTITION BY CHANNEL ORDER BY DATE_FORMAT(DEPLOYMENT_DATE, '%Y-%m')) AS prev_month_deployments,
    ROUND(100.0 * (COUNT(*) - LAG(COUNT(*)) OVER (PARTITION BY CHANNEL ORDER BY DATE_FORMAT(DEPLOYMENT_DATE, '%Y-%m'))) / 
        NULLIF(LAG(COUNT(*)) OVER (PARTITION BY CHANNEL ORDER BY DATE_FORMAT(DEPLOYMENT_DATE, '%Y-%m')), 0), 2) AS deployment_growth_pct
    
FROM vvd_campaign_deployments
WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date
GROUP BY DATE_FORMAT(DEPLOYMENT_DATE, '%Y-%m'), CHANNEL
ORDER BY year_month, CHANNEL;

-- 7. Executive Summary Query
SELECT 
    'VVD Campaign Analysis Summary' AS report_title,
    CURRENT_TIMESTAMP() AS generated_at,
    
    -- Overall metrics
    (SELECT COUNT(DISTINCT TACTIC_ID) FROM vvd_campaign_deployments WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date) AS total_deployments,
    (SELECT COUNT(DISTINCT CLIENT_ID) FROM vvd_campaign_deployments WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date) AS unique_clients,
    (SELECT COUNT(DISTINCT CAMPAIGN_ID) FROM vvd_campaign_deployments WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date) AS total_campaigns,
    
    -- Performance metrics
    (SELECT ROUND(100.0 * AVG(RESPONSE_FLAG), 2) FROM vvd_campaign_deployments WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date) AS overall_response_rate,
    (SELECT ROUND(100.0 * AVG(CONVERSION_FLAG), 2) FROM vvd_campaign_deployments WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date) AS overall_conversion_rate,
    (SELECT SUM(REVENUE) FROM vvd_campaign_deployments WHERE DEPLOYMENT_DATE BETWEEN @analysis_start_date AND @analysis_end_date) AS total_revenue;

-- 8. Top Performing Campaigns
SELECT 
    'Top 10 Campaigns by Conversion Rate' AS report_section,
    CAMPAIGN_NAME,
    CHANNEL,
    total_deployments,
    unique_clients,
    response_rate,
    conversion_rate,
    total_revenue
FROM vvd_campaign_performance
WHERE total_deployments >= 1000  -- Minimum volume threshold
ORDER BY conversion_rate DESC
LIMIT 10;

-- 9. Optimal Contact Frequency Recommendations
SELECT 
    'Optimal Contact Frequency by Channel' AS report_section,
    CHANNEL,
    contacts_last_30d AS optimal_contacts_per_30d,
    response_rate,
    conversion_rate,
    avg_revenue,
    sample_size
FROM vvd_optimal_frequency
WHERE revenue_rank = 1  -- Best performing frequency per channel
ORDER BY CHANNEL;