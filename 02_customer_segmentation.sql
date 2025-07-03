-- ===========================================================
-- Filename: 02_customer_analysis.sql
-- Description: Customer behavior, segmentation & revenue contribution
-- Objective: Identify top customers, customer value segments, and behavioral KPIs
-- Author: Reem Bouqueau
-- Date: 2025-07-01
-- ===========================================================

-- Total Number of Unique Customers
SELECT 
    COUNT(DISTINCT CustomerID) AS Total_Customers
FROM 
    customers;

-- Top 10 Customers by Revenue
SELECT 
    t.CustomerID,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Revenue
FROM 
    transactions t
WHERE 
    t.Quantity > 0
GROUP BY 
    t.CustomerID
ORDER BY 
    Total_Revenue DESC
LIMIT 10;

-- Average Revenue Per Customer
SELECT 
    ROUND(SUM(t.Quantity * t.UnitPrice) / COUNT(DISTINCT t.CustomerID), 2) AS Avg_Revenue_Per_Customer
FROM 
    transactions t
WHERE 
    t.Quantity > 0;

-- Customer Segmentation by Revenue
SELECT 
    CustomerID,
    CASE 
        WHEN Revenue >= 10000 THEN 'High Value'
        WHEN Revenue >= 1000 THEN 'Mid Value'
        ELSE 'Low Value'
    END AS Segment,
    Revenue
FROM (
    SELECT 
        CustomerID,
        ROUND(SUM(Quantity * UnitPrice), 2) AS Revenue
    FROM 
        transactions
    WHERE 
        Quantity > 0
    GROUP BY 
        CustomerID
) AS customer_revenue
ORDER BY 
    Revenue DESC;
    
    

-- Top 5 Countries by Active Customers
SELECT 
    c.Country,
    COUNT(DISTINCT c.CustomerID) AS Active_Customers
FROM 
    customers c
JOIN 
    transactions t ON c.CustomerID = t.CustomerID
GROUP BY 
    c.Country
ORDER BY 
    Active_Customers DESC
LIMIT 5;
