CREATE TABLE dbo.SalesOrderHeader (
    SalesOrderID   INT          NOT NULL,
    CustomerID     INT          NOT NULL,
    TerritoryID    INT          NULL,
    OrderDate      DATETIME     NOT NULL,
    TotalDue       MONEY        NOT NULL,
    Status         TINYINT      NOT NULL,
    IsOnlineOrder  BIT          NOT NULL DEFAULT 0
);

CREATE TABLE dbo.SalesTerritory (
    TerritoryID    INT          NOT NULL,
    TerritoryName  NVARCHAR(50) NOT NULL,
    TerritoryGroup NVARCHAR(50) NULL
);

CREATE TABLE dbo.Customer (
    CustomerID    INT           NOT NULL,
    FirstName     NVARCHAR(100) NOT NULL,
    LastName      NVARCHAR(100) NOT NULL,
    EmailAddress  NVARCHAR(200) NULL,
    CustNo        VARCHAR(20)   NULL
);
