-- ===========================================================
-- Filename: 11_return_vs_sales.sql
-- Description: Return rate by country compared to sales
-- Objective: Analyze return risk and efficiency per geographic market
-- Author: Reem Bouqueau
-- ===========================================================

SELECT 
    c.Country,
    COUNT(DISTINCT s.InvoiceNo) AS Total_Sales_Orders,
    COUNT(DISTINCT r.InvoiceNo) AS Return_Orders,
    ROUND((COUNT(DISTINCT r.InvoiceNo) / COUNT(DISTINCT s.InvoiceNo)) * 100, 2) AS Return_Rate_Percent
FROM transactions s
JOIN customers c ON s.CustomerID = c.CustomerID
LEFT JOIN returns r ON s.InvoiceNo = r.InvoiceNo
GROUP BY c.Country
ORDER BY Return_Rate_Percent DESC;
