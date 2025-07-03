-- ===========================================================
-- Filename: 08_geo_performance.sql
-- Description: Geographic sales performance analysis
-- Objective: Identify revenue and sales distribution by country
-- Author: Reem Bouqueau
-- Date: 2025-07-01
-- ===========================================================

SELECT 
    c.Country,
    COUNT(DISTINCT t.InvoiceNo) AS Total_Orders,
    COUNT(DISTINCT t.CustomerID) AS Unique_Customers,
    SUM(t.Quantity) AS Units_Sold,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Revenue,
    ROUND(SUM(t.Quantity * t.UnitPrice) / COUNT(DISTINCT t.InvoiceNo), 2) AS Avg_Order_Value
FROM transactions t
JOIN customers c ON t.CustomerID = c.CustomerID
GROUP BY c.Country
ORDER BY Total_Revenue DESC;
