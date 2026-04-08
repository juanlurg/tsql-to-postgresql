SELECT OrderID FROM dbo.Orders WHERE DATEPART(weekday, OrderDate) = 2;
