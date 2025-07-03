-- ===========================================================
-- Filename: 09_top_customers.sql
-- Description: Analysis of top customers by revenue and order behavior
-- Objective: Identify highest-spending and most loyal customers
-- Author: Reem Bouqueau
-- ===========================================================

SELECT 
    t.CustomerID,
    c.Country,
    COUNT(DISTINCT t.InvoiceNo) AS Orders,
    SUM(t.Quantity) AS Total_Units,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Spent,
    ROUND(SUM(t.Quantity * t.UnitPrice) / COUNT(DISTINCT t.InvoiceNo), 2) AS Avg_Spend_Per_Order
FROM transactions t
JOIN customers c ON t.CustomerID = c.CustomerID
GROUP BY t.CustomerID, c.Country
ORDER BY Total_Spent DESC
LIMIT 20;
