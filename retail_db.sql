CREATE DATABASE retail_db;
USE retail_db;
SELECT DATABASE();

-- Customers Table
CREATE TABLE customers (
    CustomerID INT PRIMARY KEY,
    Country VARCHAR(50)
);

-- Products Tbable 
CREATE TABLE products (
    StockCode VARCHAR(20) PRIMARY KEY,
    Description VARCHAR(255)
);

-- Invoices Table 
CREATE TABLE invoices (
    InvoiceNo VARCHAR(20) PRIMARY KEY,
    InvoiceDate DATETIME
);

-- Transactions Table 
CREATE TABLE transactions (
    TransactionID INT AUTO_INCREMENT PRIMARY KEY,
    InvoiceNo VARCHAR(20),
    StockCode VARCHAR(20),
    CustomerID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    FOREIGN KEY (InvoiceNo) REFERENCES invoices(InvoiceNo),
    FOREIGN KEY (StockCode) REFERENCES products(StockCode),
    FOREIGN KEY (CustomerID) REFERENCES customers(CustomerID)
);

-- Returns Table 
CREATE TABLE returns (
    ReturnID INT AUTO_INCREMENT PRIMARY KEY,
    InvoiceNo VARCHAR(20),
    StockCode VARCHAR(20),
    CustomerID INT,
    Quantity INT,
    Unit_Price DECIMAL(10, 2)
);





