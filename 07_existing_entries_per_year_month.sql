-- ===========================================================
-- Filename: 07_existing_entries_per_year_month.sql
-- Description: Invoice Date, total entries per year and month 
-- Objective: See how many entries exist per year and month
-- Author: Reem Bouqueau
-- Date: 2025-07-01
-- ===========================================================

-- View Year and Month Breakdown (Entries Grouped by Year and Month)
SELECT 
    YEAR(InvoiceDate) AS year,            -- Extracts the year part from the InvoiceDate (e.g., 2022)
    MONTH(InvoiceDate) AS month,          -- Extracts the month part (e.g., 1 for January)
    COUNT(*) AS entry_count               -- Counts how many invoices occurred in that year/month
FROM 
    invoices                               -- The table you're analyzing
GROUP BY 
    YEAR(InvoiceDate),                    -- Groups results first by year
    MONTH(InvoiceDate)                    -- Then by month
ORDER BY 
    year,                                 -- Orders first by year (ascending)
    month;                                -- Then orders by month from January (1) to December (12)


-- Count How Many Unique Year–Month Periods Exist
SELECT 
    COUNT(DISTINCT DATE_FORMAT(InvoiceDate, '%Y-%m')) AS num_unique_months -- Formats the date as 'YYYY-MM' and counts how many unique values exist
    FROM 
    invoices; -- From the invoices table
    
    -- Group by Month Only (Across All Years) – Ordered Jan to Dec
    SELECT 
    MONTH(InvoiceDate) AS month,          -- Extracts just the month part (1 = Jan, ..., 12 = Dec)
    COUNT(*) AS entry_count               -- Counts how many invoices happened in that month (across all years)
FROM 
    invoices
GROUP BY 
    MONTH(InvoiceDate)
ORDER BY 
    month;                                -- Ensures results go from January to December
    
SELECT * FROM transactions as t
JOIN invoices as i ON t.InvoiceNo = i.InvoiceNo;

SELECT DISTINCT 
    DATE_FORMAT(i.InvoiceDate, '%Y-%m') AS Month
FROM 
    transactions AS t
JOIN 
    invoices AS i ON t.InvoiceNo = i.InvoiceNo
WHERE 
    t.Quantity > 0
ORDER BY 
    Month;
    
    

