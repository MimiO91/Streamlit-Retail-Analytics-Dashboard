-- ===========================================================
-- Filename: 12_sales_kpis.sql
-- Description: Overall sales KPIs summary for business snapshot
-- Objective: Present key metrics on sales volume, customers, and revenue
-- Author: Reem Bouqueau
-- ===========================================================

SELECT
    COUNT(DISTINCT t.InvoiceNo) AS Total_Orders,
    COUNT(DISTINCT t.CustomerID) AS Total_Customers,
    COUNT(DISTINCT t.StockCode) AS Unique_Products,
    SUM(t.Quantity) AS Total_Units_Sold,
    ROUND(SUM(t.Quantity * t.UnitPrice), 2) AS Total_Revenue,
    ROUND(SUM(t.Quantity * t.UnitPrice) / COUNT(DISTINCT t.InvoiceNo), 2) AS Avg_Order_Value
FROM transactions t;
