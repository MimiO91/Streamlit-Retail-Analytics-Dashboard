-- ===========================================================
-- Filename: 03_product_performance.sql
-- Description: Top products, unit profitability & repeat sales analysis
-- Objective: Identify top-performing SKUs, repeat purchases, and revenue drivers
-- Author: Reem Bouqueau
-- Date: 2025-07-01
-- ===========================================================

-- Top 10 Products by Revenue
SELECT 
    p.StockCode,
    p.Description,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Revenue
FROM 
    transactions t
JOIN 
    products p ON t.StockCode = p.StockCode
WHERE 
    t.Quantity > 0
GROUP BY 
    p.StockCode, p.Description
ORDER BY 
    Total_Revenue DESC
LIMIT 10;

-- Top 10 Products by Revenue & Unit Price
SELECT 
    p.StockCode,
    p.Description,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Revenue,
    SUM(t.Quantity) AS Total_Units_Sold,
    ROUND(AVG(t.UnitPrice), 2) AS Avg_Unit_Price
FROM 
    transactions t
JOIN 
    products p ON t.StockCode = p.StockCode
WHERE 
    t.Quantity > 0
GROUP BY 
    p.StockCode, p.Description
ORDER BY 
    Total_Revenue DESC
LIMIT 10;

-- Top 10 Products by Units Sold
SELECT 
    p.StockCode,
    p.Description,
    SUM(t.Quantity) AS Total_Units_Sold
FROM 
    transactions t
JOIN 
    products p ON t.StockCode = p.StockCode
WHERE 
    t.Quantity > 0
GROUP BY 
    p.StockCode, p.Description
ORDER BY 
    Total_Units_Sold DESC
LIMIT 10;

-- Product Unit Price Range (Min–Max)
SELECT 
    p.StockCode,
    p.Description,
    MIN(t.UnitPrice) AS Min_UnitPrice,
    MAX(t.UnitPrice) AS Max_UnitPrice
FROM 
    transactions t
JOIN 
    products p ON t.StockCode = p.StockCode
GROUP BY 
    p.StockCode, p.Description
ORDER BY 
    Max_UnitPrice DESC
LIMIT 10;

-- Most Frequently Ordered Products (by Invoice Count)
SELECT 
    p.StockCode,
    p.Description,
    COUNT(DISTINCT t.InvoiceNo) AS Invoice_Count
FROM 
    transactions t
JOIN 
    products p ON t.StockCode = p.StockCode
WHERE 
    t.Quantity > 0
GROUP BY 
    p.StockCode, p.Description
ORDER BY 
    Invoice_Count DESC
LIMIT 10;

-- Product Return Rate (% of units returned)
SELECT 
    p.StockCode,
    p.Description,
    ABS(SUM(r.Quantity)) AS Units_Returned,
    SUM(t.Quantity) AS Units_Sold,
    ROUND(ABS(SUM(r.Quantity)) / SUM(t.Quantity) * 100, 2) AS Return_Rate_Percent
FROM 
    transactions t
JOIN 
    products p ON t.StockCode = p.StockCode
LEFT JOIN 
    returns r ON t.StockCode = r.StockCode AND t.CustomerID = r.CustomerID
WHERE 
    t.Quantity > 0
GROUP BY 
    p.StockCode, p.Description
HAVING 
    Units_Sold > 0
ORDER BY 
    Return_Rate_Percent DESC
LIMIT 10;
