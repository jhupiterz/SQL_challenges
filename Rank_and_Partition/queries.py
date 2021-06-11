# pylint:disable=C0111,C0103

def order_rank_per_customer(db):
    query = """SELECT Orders.OrderID, Customers.CustomerID, Orders.OrderDate,
	        RANK() OVER (
            PARTITION BY Orders.CustomerID
            ORDER BY Orders.OrderDate
            ) AS order_rank
            FROM Customers
            JOIN Orders ON Customers.CustomerID = Orders.CustomerID
            ORDER BY Customers.CustomerID"""
    db.execute(query)
    results = db.fetchall()
    return results

def order_cumulative_amount_per_customer(db):
    query = """SELECT DISTINCT Orders.OrderID, Customers.CustomerID, Orders.OrderDate,
	        SUM(OrderDetails.Quantity * OrderDetails.UnitPrice) OVER (
            PARTITION BY Orders.CustomerID
            ORDER BY Orders.OrderDate
            ) AS cumulative_amount
            FROM Orders
            JOIN Customers ON Orders.CustomerID = Customers.CustomerID
            JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID"""
    db.execute(query)
    results = db.fetchall()
    return results
