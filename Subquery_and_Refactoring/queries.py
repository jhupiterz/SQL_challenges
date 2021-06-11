# pylint:disable=C0111,C0103

def get_average_purchase(db):
    # return the average amount spent per order for each customer ordered by customer ID
    query = """
            WITH amounts_per_customer AS (
	        SELECT DISTINCT Orders.CustomerID,
                SUM(OrderDetails.Quantity*OrderDetails.UnitPrice) OVER (
                PARTITION BY Orders.CustomerID 
	        ) AS total_amount_per_customer
            FROM Orders
            JOIN OrderDetails ON OrderDetails.OrderID = Orders.OrderID 
            ORDER BY Orders.CustomerID
            )
            SELECT Orders.CustomerID, ROUND(total_amount_per_customer/COUNT(Orders.CustomerID),1)
            FROM amounts_per_customer
            JOIN Orders ON Orders.CustomerID = amounts_per_customer.CustomerID
            GROUP BY Orders.CustomerID """
    db.execute(query)
    results = db.fetchall()
    return results

def get_general_avg_order(db):
    # return the average amount spent per order
    query = """
            WITH all_amounts AS (
            SELECT DISTINCT Orders.OrderID,
            SUM(OrderDetails.Quantity*OrderDetails.UnitPrice) OVER (
            PARTITION BY Orders.OrderID) AS amount_per_order
            FROM Orders
            JOIN OrderDetails ON OrderDetails.OrderID = Orders.OrderID
            )
            SELECT SUM(amount_per_order)/COUNT(OrderID)
            FROM all_amounts"""
    db.execute(query)
    results = db.fetchall()
    return round(results[0][0],2)

def best_customers(db):
    # return the customers who have an average purchase greater than the general average purchase
    query = """
            WITH avg_amounts_per_customer AS (
	            WITH amounts_per_customer AS (
		        SELECT DISTINCT Orders.CustomerID,
			        SUM(OrderDetails.Quantity*OrderDetails.UnitPrice) OVER (
			        PARTITION BY Orders.CustomerID 
		        ) AS total_amount_per_customer
		        FROM Orders
		    JOIN OrderDetails ON OrderDetails.OrderID = Orders.OrderID 
		    ORDER BY Orders.CustomerID
	            )
	            SELECT Orders.CustomerID, ROUND(total_amount_per_customer/COUNT(Orders.CustomerID),2) AS avg_amount_per_customer
                FROM amounts_per_customer
                JOIN Orders ON Orders.CustomerID = amounts_per_customer.CustomerID
                GROUP BY Orders.CustomerID
            )
            SELECT CustomerID, avg_amount_per_customer
            FROM avg_amounts_per_customer
            WHERE avg_amount_per_customer > ?
            ORDER BY avg_amount_per_customer DESC"""
    db.execute(query, (get_general_avg_order(db),))
    results = db.fetchall()
    return results

def top_ordered_product_per_customer(db):
    # return the list of the top ordered product by each customer based on the total ordered amount
    query = """
            WITH all_product_amounts_per_customer AS (
                WITH amount_per_product AS (
                    SELECT Orders.CustomerID, OrderDetails.ProductID, OrderDetails.Quantity*OrderDetails.UnitPrice AS product_price 
                    FROM Orders
                    JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID
                    ORDER BY Orders.CustomerID
                )
                SELECT DISTINCT CustomerID, ProductID,
                    SUM(amount_per_product.product_price) OVER (
                        PARTITION BY CustomerID, ProductID
                    ) AS ordered_amount_per_product
                FROM amount_per_product
                ORDER BY CustomerID, ordered_amount_per_product DESC
            )
            SELECT CustomerID, ProductID,
	            MAX(ordered_amount_per_product) OVER (
		            PARTITION BY CustomerID
		        ) AS total_product_amount_per_customer
            FROM all_product_amounts_per_customer
            GROUP BY CustomerID
            ORDER BY total_product_amount_per_customer DESC"""
    db.execute(query)
    response = db.fetchall()
    return response

def average_number_of_days_between_orders(db):
    # return the average number of days between two consecutive orders of the same customer
    query = """
            WITH filtered_times_between_orders AS (
                WITH all_time_between_two_orders AS (
                    SELECT CustomerID, OrderDate, 
                        JULIANDAY(OrderDate) - LAG(JULIANDAY(OrderDate), 1, 0) OVER(
                            PARTITION BY CustomerID) AS time_between_two_orders
                    FROM Orders
                    ORDER BY CustomerID
                )
                SELECT CustomerID, OrderDate, time_between_two_orders
                FROM all_time_between_two_orders
                WHERE time_between_two_orders < 2000
            )
            SELECT ROUND(SUM(time_between_two_orders)/COUNT(CustomerID))
            FROM filtered_times_between_orders"""
    db.execute(query)
    response = db.fetchall()
    return int(response[0][0])
