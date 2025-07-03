-- ===========================================================
-- Filename: 10_low_products.sql
-- Description: Underperforming products with low sales and revenue
-- Objective: Identify products with minimal sales for possible action
-- Author: Reem Bouqueau
-- ===========================================================

SELECT 
    t.StockCode,
    p.Description,
    SUM(t.Quantity) AS Total_Units_Sold,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Revenue
FROM transactions t
JOIN products p ON t.StockCode = p.StockCode
GROUP BY t.StockCode, p.Description
HAVING Total_Units_Sold < 5 OR Total_Revenue < 10
ORDER BY Total_Revenue ASC;

SELECT 
    t.StockCode,
    p.Description,
    SUM(t.Quantity) AS Total_Units_Sold,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Revenue
FROM transactions t
JOIN products p ON t.StockCode = p.StockCode
GROUP BY t.StockCode, p.Description
HAVING Total_Units_Sold < 5 OR Total_Revenue < 10
ORDER BY Total_Revenue DESC;
