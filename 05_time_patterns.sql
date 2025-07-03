-- ===========================================================
-- Filename: 05_time_patterns.sql
-- Description: Monthly and weekly KPIs for sales, returns, and customer activity
-- Objective: Analyze time-based performance patterns to identify trends, spikes, and seasonality
-- Author: Reem Bouqueau
-- ===========================================================

-- Monthly Revenue Trend
SELECT
    DATE_FORMAT(i.InvoiceDate, '%Y-%m') AS Month,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Monthly_Revenue
FROM 
    transactions t
JOIN 
    invoices i ON t.InvoiceNo = i.InvoiceNo
WHERE 
    t.Quantity > 0
GROUP BY 
    Month
ORDER BY 
    Month;

-- Weekly Revenue Trend
SELECT
    DATE_FORMAT(i.InvoiceDate, '%Y-%u') AS Week,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Weekly_Revenue
FROM 
    transactions t
JOIN 
    invoices i ON t.InvoiceNo = i.InvoiceNo
WHERE 
    t.Quantity > 0
GROUP BY 
    Week
ORDER BY 
    Week;

-- Monthly Active Customers
SELECT
    DATE_FORMAT(i.InvoiceDate, '%Y-%m') AS Month,
    COUNT(DISTINCT t.CustomerID) AS Active_Customers
FROM 
    transactions t
JOIN 
    invoices i ON t.InvoiceNo = i.InvoiceNo
WHERE 
    t.Quantity > 0
GROUP BY 
    Month
ORDER BY 
    Month;
-- ===========================================================
-- Filename: 05_time_patterns.sql
-- Description: Monthly and weekly KPIs for sales, returns, and customer activity
-- Objective: Analyze time-based performance patterns to identify trends, spikes, and seasonality
-- Author: Reem Bouqueau
-- ===========================================================

-- Monthly Revenue Trend
SELECT
    DATE_FORMAT(i.InvoiceDate, '%Y-%m') AS Month,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Monthly_Revenue
FROM 
    transactions t
JOIN 
    invoices i ON t.InvoiceNo = i.InvoiceNo
WHERE 
    t.Quantity > 0
GROUP BY 
    Month
ORDER BY 
    Month;

-- Weekly Revenue Trend
SELECT
    DATE_FORMAT(i.InvoiceDate, '%Y-%u') AS Week,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Weekly_Revenue
FROM 
    transactions t
JOIN 
    invoices i ON t.InvoiceNo = i.InvoiceNo
WHERE 
    t.Quantity > 0
GROUP BY 
    Week
ORDER BY 
    Week;

-- Monthly Active Customers
SELECT
    DATE_FORMAT(i.InvoiceDate, '%Y-%m') AS Month,
    COUNT(DISTINCT t.CustomerID) AS Active_Customers
FROM 
    transactions t
JOIN 
    invoices i ON t.InvoiceNo = i.InvoiceNo
WHERE 
    t.Quantity > 0
GROUP BY 
    Month
ORDER BY 
    Month;
    
-- Monthly Revenue Trend
SELECT 
    DATE_FORMAT(InvoiceDate, '%Y-%m') AS Return_Month,
    COUNT(DISTINCT InvoiceNo) AS Return_Orders,
    SUM(ABS(Quantity)) AS Units_Returned,
    ROUND(SUM(ABS(Quantity * Unit_Price)), 2) AS Total_Return_Value
FROM returns
GROUP BY Return_Month
ORDER BY Return_Month;

-- Weekly Return Trend

SELECT 
    YEAR(InvoiceDate) AS Year,
    WEEK(InvoiceDate, 3) AS Week_ISO,
    COUNT(DISTINCT InvoiceNo) AS Return_Orders,
    SUM(ABS(Quantity)) AS Units_Returned,
    ROUND(SUM(ABS(Quantity * Unit_Price)), 2) AS Total_Return_Value
FROM returns
GROUP BY Year, Week_ISO
ORDER BY Year, Week_ISO;
