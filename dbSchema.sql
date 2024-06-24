-- Create Categories table
CREATE TABLE Categories (
    category_id INT PRIMARY KEY IDENTITY(1,1),
    category_name VARCHAR(255) NOT NULL
);
GO

CREATE TABLE Categories_Staging (
    category_id INT,
    category_name VARCHAR(255) NOT NULL
);
GO

-- Create Products table
CREATE TABLE Products (
    product_id INT PRIMARY KEY IDENTITY(1,1),
    product_name VARCHAR(255) NOT NULL,
    category_id INT,
    price DECIMAL(10, 2) NOT NULL,
    description NVARCHAR(MAX),
    image_url VARCHAR(255),
    date_added DATE NOT NULL,
    FOREIGN KEY (category_id) REFERENCES Categories(category_id)
);
GO

CREATE TABLE Products_Staging (
    product_id INT,
    product_name VARCHAR(255) NOT NULL,
    category_id INT,
    price DECIMAL(10, 2) NOT NULL,
    description NVARCHAR(MAX),
    image_url VARCHAR(255),
    date_added DATE NOT NULL,
    FOREIGN KEY (category_id) REFERENCES Categories(category_id)
);
GO

-- Create Orders table
CREATE TABLE Orders (
    order_id INT PRIMARY KEY IDENTITY(1,1),
    order_date DATE NOT NULL,
    customer_name VARCHAR(255) NOT NULL
);
GO

CREATE TABLE Orders_Staging (
    order_id INT,
    order_date DATE NOT NULL,
    customer_name VARCHAR(255) NOT NULL
);
GO

-- Create Order_Products table
CREATE TABLE Order_Products (
    order_id INT,
    product_id INT,
    quantity INT NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);
GO

CREATE TABLE Order_Products_Staging (
    order_id INT,
    product_id INT,
    quantity INT NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);
GO

-- Create CategoryChangeLog table
CREATE TABLE CategoryChangeLog (
    ChangeLogId INT IDENTITY(1,1) PRIMARY KEY,
    Operation NVARCHAR(10),
    category_id INT,
    category_name VARCHAR(255),
    ChangeTime DATETIME
);
GO

-- Create ProductChangeLog table
CREATE TABLE ProductChangeLog (
    ChangeLogId INT IDENTITY(1,1) PRIMARY KEY,
    Operation NVARCHAR(10),
    Id INT,
    product_name VARCHAR(255),
    category_id INT,
    price DECIMAL(10, 2),
    description TEXT,
    image_url VARCHAR(255),
    date_added DATE,
    ChangeTime DATETIME
);
GO

-- Create OrderChangeLog table
CREATE TABLE OrderChangeLog (
    ChangeLogId INT IDENTITY(1,1) PRIMARY KEY,
    Operation NVARCHAR(10),
    order_id INT,
    order_date DATE,
    customer_name VARCHAR(255),
    ChangeTime DATETIME
);
GO

-- Create OrderProductsChangeLog table
CREATE TABLE OrderProductsChangeLog (
    ChangeLogId INT IDENTITY(1,1) PRIMARY KEY,
    Operation NVARCHAR(10),
    order_id INT,
    product_id INT,
    quantity INT,
    ChangeTime DATETIME
);
GO

-- Enable Change Tracking on the database
ALTER DATABASE EmpowerDemoDB
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
GO

-- Enable Change Tracking on the Categories table
ALTER TABLE dbo.Categories
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

-- Enable Change Tracking on the Products table
ALTER TABLE dbo.Products
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

-- Enable Change Tracking on the Orders table
ALTER TABLE dbo.Orders
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

-- Enable Change Tracking on the Order_Products table
ALTER TABLE dbo.Order_Products
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

-- Create trigger to log changes in the Categories table
CREATE TRIGGER trgCategoryAfterInsertUpdateDelete
ON dbo.Categories
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    -- Log INSERT operations
    INSERT INTO CategoryChangeLog (Operation, category_id, category_name, ChangeTime)
    SELECT 'INSERT', category_id, category_name, GETDATE() FROM inserted;

    -- Log DELETE operations
    INSERT INTO CategoryChangeLog (Operation, category_id, category_name, ChangeTime)
    SELECT 'DELETE', category_id, category_name, GETDATE() FROM deleted;

    -- Log UPDATE operations (Old values)
    INSERT INTO CategoryChangeLog (Operation, category_id, category_name, ChangeTime)
    SELECT 'UPDATE-OLD', category_id, category_name, GETDATE() FROM deleted;

    -- Log UPDATE operations (New values)
    INSERT INTO CategoryChangeLog (Operation, category_id, category_name, ChangeTime)
    SELECT 'UPDATE-NEW', category_id, category_name, GETDATE() FROM inserted;
END
GO

-- Create trigger to log changes in the Products table
CREATE TRIGGER trgProductAfterInsertUpdateDelete
ON dbo.Products
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    -- Log INSERT operations
    INSERT INTO ProductChangeLog (Operation, Id, product_name, category_id, price, description, image_url, date_added, ChangeTime)
    SELECT 'INSERT', product_id, product_name, category_id, price, description, image_url, date_added, GETDATE() FROM inserted;

    -- Log DELETE operations
    INSERT INTO ProductChangeLog (Operation, Id, product_name, category_id, price, description, image_url, date_added, ChangeTime)
    SELECT 'DELETE', product_id, product_name, category_id, price, description, image_url, date_added, GETDATE() FROM deleted;

    -- Log UPDATE operations (Old values)
    INSERT INTO ProductChangeLog (Operation, Id, product_name, category_id, price, description, image_url, date_added, ChangeTime)
    SELECT 'UPDATE-OLD', product_id, product_name, category_id, price, description, image_url, date_added, GETDATE() FROM deleted;

    -- Log UPDATE operations (New values)
    INSERT INTO ProductChangeLog (Operation, Id, product_name, category_id, price, description, image_url, date_added, ChangeTime)
    SELECT 'UPDATE-NEW', product_id, product_name, category_id, price, description, image_url, date_added, GETDATE() FROM inserted;
END
GO

-- Create trigger to log changes in the Orders table
CREATE TRIGGER trgOrdersAfterInsertUpdateDelete
ON dbo.Orders
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    -- Log INSERT operations
    INSERT INTO OrderChangeLog (Operation, order_id, order_date, customer_name, ChangeTime)
    SELECT 'INSERT', order_id, order_date, customer_name, GETDATE() FROM inserted;

    -- Log DELETE operations
    INSERT INTO OrderChangeLog (Operation, order_id, order_date, customer_name, ChangeTime)
    SELECT 'DELETE', order_id, order_date, customer_name, GETDATE() FROM deleted;

    -- Log UPDATE operations (Old values)
    INSERT INTO OrderChangeLog (Operation, order_id, order_date, customer_name, ChangeTime)
    SELECT 'UPDATE-OLD', order_id, order_date, customer_name, GETDATE() FROM deleted;

    -- Log UPDATE operations (New values)
    INSERT INTO OrderChangeLog (Operation, order_id, order_date, customer_name, ChangeTime)
    SELECT 'UPDATE-NEW', order_id, order_date, customer_name, GETDATE() FROM inserted;
END
GO

-- Create trigger to log changes in the Order_Products table
CREATE TRIGGER trgOrderProductsAfterInsertUpdateDelete
ON dbo.Order_Products
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    -- Log INSERT operations
    INSERT INTO OrderProductsChangeLog (Operation, order_id, product_id, quantity, ChangeTime)
    SELECT 'INSERT', order_id, product_id, quantity, GETDATE() FROM inserted;

    -- Log DELETE operations
    INSERT INTO OrderProductsChangeLog (Operation, order_id, product_id, quantity, ChangeTime)
    SELECT 'DELETE', order_id, product_id, quantity, GETDATE() FROM deleted;

    -- Log UPDATE operations (Old values)
    INSERT INTO OrderProductsChangeLog (Operation, order_id, product_id, quantity, ChangeTime)
    SELECT 'UPDATE-OLD', order_id, product_id, quantity, GETDATE() FROM deleted;

    -- Log UPDATE operations (New values)
    INSERT INTO OrderProductsChangeLog (Operation, order_id, product_id, quantity, ChangeTime)
    SELECT 'UPDATE-NEW', order_id, product_id, quantity, GETDATE() FROM inserted;
END
GO