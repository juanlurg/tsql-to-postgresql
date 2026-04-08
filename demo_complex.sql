-- ============================================================
-- Real-world e-commerce order analytics report (T-SQL / SQL Server)
-- Covers: CTEs, window functions, DATEPART, DATEADD, GETDATE,
--         ISNULL, CONVERT, CHARINDEX, NOLOCK hints, TOP N,
--         CROSS APPLY, nvarchar types, CASE expressions
-- ============================================================

WITH CustomerSegments AS (
    SELECT
        c.CustomerID,
        c.AccountNumber,
        ISNULL(c.CompanyName, c.LastName + N', ' + c.FirstName) AS DisplayName,
        CONVERT(NVARCHAR(10), c.ModifiedDate, 120)               AS LastModified,
        CASE
            WHEN c.CustomerType = N'S' THEN N'Store'
            WHEN c.CustomerType = N'I' THEN N'Individual'
            ELSE N'Unknown'
        END AS CustomerCategory,
        CHARINDEX(N'@', c.EmailAddress)                          AS AtSignPos
    FROM dbo.Customer c WITH (NOLOCK)
    WHERE c.rowguid IS NOT NULL
),
OrderMetrics AS (
    SELECT
        oh.CustomerID,
        oh.SalesOrderID,
        oh.OrderDate,
        oh.DueDate,
        oh.ShipDate,
        oh.TotalDue,
        oh.Freight,
        oh.SubTotal,
        ISNULL(oh.Comment, N'')                                  AS OrderComment,
        DATEPART(YEAR,  oh.OrderDate)                            AS OrderYear,
        DATEPART(MONTH, oh.OrderDate)                            AS OrderMonth,
        DATEPART(WEEK,  oh.OrderDate)                            AS OrderWeek,
        DATEDIFF(DAY, oh.OrderDate, ISNULL(oh.ShipDate, GETDATE())) AS DaysToShip,
        SUM(oh.TotalDue) OVER (
            PARTITION BY oh.CustomerID
            ORDER BY oh.OrderDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS RunningTotal,
        ROW_NUMBER() OVER (
            PARTITION BY oh.CustomerID
            ORDER BY oh.OrderDate DESC
        ) AS OrderRank,
        LAG(oh.TotalDue, 1, 0) OVER (
            PARTITION BY oh.CustomerID
            ORDER BY oh.OrderDate
        ) AS PrevOrderAmount
    FROM dbo.SalesOrderHeader oh WITH (NOLOCK)
    WHERE oh.OrderDate >= DATEADD(MONTH, -24, GETDATE())
      AND oh.Status IN (5, 6)
),
LineItemSummary AS (
    SELECT
        od.SalesOrderID,
        COUNT(*)                                    AS LineCount,
        SUM(od.OrderQty)                            AS TotalQty,
        SUM(od.LineTotal)                           AS LineTotal,
        MAX(CONVERT(NVARCHAR(50), p.Name))          AS TopProductName,
        STRING_AGG(CONVERT(NVARCHAR(50), p.Name), N', ')
            WITHIN GROUP (ORDER BY od.LineTotal DESC) AS ProductList
    FROM dbo.SalesOrderDetail od WITH (NOLOCK)
    INNER JOIN dbo.Product p WITH (NOLOCK)
        ON od.ProductID = p.ProductID
    GROUP BY od.SalesOrderID
)
SELECT TOP 1000
    cs.DisplayName,
    cs.CustomerCategory,
    cs.AccountNumber,
    om.SalesOrderID,
    om.OrderDate,
    om.OrderYear,
    om.OrderMonth,
    om.OrderWeek,
    om.TotalDue,
    om.Freight,
    om.SubTotal,
    om.DaysToShip,
    om.RunningTotal,
    om.OrderRank,
    om.PrevOrderAmount,
    om.TotalDue - om.PrevOrderAmount                    AS DeltaVsPrev,
    li.LineCount,
    li.TotalQty,
    li.TopProductName,
    li.ProductList,
    CASE
        WHEN om.DaysToShip <= 2  THEN N'Express'
        WHEN om.DaysToShip <= 7  THEN N'Standard'
        WHEN om.DaysToShip > 7   THEN N'Late'
        ELSE N'Not Shipped'
    END AS ShippingTier,
    CAST(om.Freight AS DECIMAL(18, 4))                  AS FreightDecimal,
    CAST(om.TotalDue AS NVARCHAR(30))                   AS TotalDueStr
FROM OrderMetrics om
INNER JOIN CustomerSegments cs
    ON om.CustomerID = cs.CustomerID
LEFT JOIN LineItemSummary li
    ON om.SalesOrderID = li.SalesOrderID
WHERE om.OrderRank <= 10
ORDER BY
    om.CustomerID,
    om.OrderRank;
