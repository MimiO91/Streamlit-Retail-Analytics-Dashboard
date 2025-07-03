-- ===========================================================
-- Filename: 04_return_analysis.sql
-- Description: Return behavior, rates, and revenue impact
-- Objective: Identify return patterns and their impact on performance
-- Author: Reem Bouqueau
-- ===========================================================

-- Total Number of Return Transactions
SELECT 
    COUNT(*) AS Total_Returns
FROM 
    returns;

-- Total Units Returned and Value of Returns
SELECT 
    ABS(SUM(Quantity)) AS Total_Units_Returned,
    ROUND(SUM(ABS(Quantity * Unit_Price)), 2) AS Value_of_Returns
FROM 
    returns;

-- Top 10 Most Returned Products by Units
SELECT 
    r.StockCode,
    p.Description,
    ABS(SUM(r.Quantity)) AS Units_Returned
FROM 
    returns r
JOIN 
    products p ON r.StockCode = p.StockCode
GROUP BY 
    r.StockCode, p.Description
ORDER BY 
    Units_Returned DESC
LIMIT 10;

-- Top 10 Customers by Return Volume
SELECT 
    r.CustomerID,
    ABS(SUM(r.Quantity)) AS Total_Units_Returned,
    ROUND(SUM(ABS(r.Quantity * r.Unit_Price)), 2) AS Return_Value
FROM 
    returns r
GROUP BY 
    r.CustomerID
ORDER BY 
    Return_Value DESC
LIMIT 10;

-- Return Rate by Country (% of Units Sold Returned)
SELECT 
    c.Country,
    ABS(SUM(r.Quantity)) AS Units_Returned,
    SUM(t.Quantity) AS Units_Sold,
    ROUND(ABS(SUM(r.Quantity)) / SUM(t.Quantity) * 100, 2) AS Return_Rate_Percent
FROM 
    returns r
JOIN 
    customers c ON r.CustomerID = c.CustomerID
JOIN 
    transactions t ON r.CustomerID = t.CustomerID AND r.StockCode = t.StockCode
WHERE 
    t.Quantity > 0
GROUP BY 
    c.Country
HAVING 
    Units_Sold > 0
ORDER BY 
    Return_Rate_Percent DESC
LIMIT 10;
