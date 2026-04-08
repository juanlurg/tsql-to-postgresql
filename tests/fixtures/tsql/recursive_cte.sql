WITH Emp AS (
    SELECT EmployeeID, ManagerID, Name FROM dbo.Employees WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.EmployeeID, e.ManagerID, e.Name
    FROM dbo.Employees e
    INNER JOIN Emp m ON e.ManagerID = m.EmployeeID
)
SELECT * FROM Emp;
