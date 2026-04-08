WITH SalesCTE AS (
    SELECT
        s.SalesOrderID,
        s.CustomerID,
        s.OrderDate,
        s.TotalDue,
        ISNULL(t.TerritoryName, 'Unknown') AS TerritoryName,
        DATEPART(YEAR, s.OrderDate)        AS OrderYear,
        DATEPART(MONTH, s.OrderDate)       AS OrderMonth
    FROM dbo.SalesOrderHeader s WITH (NOLOCK)
    LEFT JOIN dbo.SalesTerritory t WITH (NOLOCK)
        ON s.TerritoryID = t.TerritoryID
    WHERE s.OrderDate >= DATEADD(DAY, -90, GETDATE())
      AND s.Status = 1
),
RevenueByMonth AS (
    SELECT
        TerritoryName,
        OrderYear,
        OrderMonth,
        SUM(TotalDue)              AS MonthlyRevenue,
        COUNT(DISTINCT CustomerID) AS UniqueCustomers
    FROM SalesCTE
    GROUP BY TerritoryName, OrderYear, OrderMonth
)
SELECT TOP 500
    r.TerritoryName,
    r.OrderYear,
    r.OrderMonth,
    r.MonthlyRevenue,
    r.UniqueCustomers,
    LAG(r.MonthlyRevenue, 12, 0) OVER (
        PARTITION BY r.TerritoryName
        ORDER BY r.OrderYear, r.OrderMonth
    ) AS PriorYearRevenue
FROM RevenueByMonth r
ORDER BY r.OrderYear DESC, r.OrderMonth DESC, r.MonthlyRevenue DESC;
