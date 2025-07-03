-- ===========================================================
-- Filename: 01_revenue_analysis.sql
-- Description: Revenue KPIs, total sales, and monthly trends
-- Objective: Track gross revenue and identify seasonal patterns
-- Author: Reem Bouqueau
-- Date: 2025-07-01
-- ===========================================================
-- Understand total revenue, seasonality, and top sales dates

-- Total revenue (excluding returns)
SELECT 
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Revenue
FROM 
    transactions t
WHERE 
    t.Quantity > 0;

-- 🔹 2. Monthly Revenue Trend
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
    
-- Average Revenue per Transaction
SELECT 
    ROUND(SUM(t.Quantity * t.UnitPrice) / COUNT(DISTINCT t.InvoiceNo), 2) AS Avg_Revenue_Per_Invoice
FROM 
    transactions t
WHERE 
    t.Quantity > 0;
    
    

