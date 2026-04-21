# STEP 0
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("SELECT * FROM sqlite_master", conn)


# STEP 1: Employees in Boston
sql = (
    "SELECT TRIM(e.firstName) AS firstName, "
    "TRIM(e.lastName) AS lastName\n"
    "FROM employees e\n"
    "JOIN offices o\n"
    "  ON e.officeCode = o.officeCode\n"
    "WHERE o.city = 'Boston'\n"
    "ORDER BY firstName, lastName"
)
df_boston = pd.read_sql(sql, conn)


# STEP 2: Offices with zero employees
sql = (
    "SELECT o.officeCode, o.city\n"
    "FROM offices o\n"
    "LEFT JOIN employees e\n"
    "  ON o.officeCode = e.officeCode\n"
    "GROUP BY o.officeCode, o.city\n"
    "HAVING COUNT(e.employeeNumber) = 0\n"
    "ORDER BY o.officeCode"
)
df_zero_emp = pd.read_sql(sql, conn)


# STEP 3: All employees + office info
sql = (
    "SELECT TRIM(e.firstName) AS firstName, "
    "TRIM(e.lastName) AS lastName, o.city, o.state\n"
    "FROM employees e\n"
    "LEFT JOIN offices o\n"
    "  ON e.officeCode = o.officeCode\n"
    "ORDER BY firstName, lastName"
)
df_employee = pd.read_sql(sql, conn)


# STEP 4: Customers with no orders
sql = (
    "SELECT TRIM(c.contactFirstName) AS contactFirstName, "
    "TRIM(c.contactLastName) AS contactLastName,\n"
    "       c.phone, c.salesRepEmployeeNumber\n"
    "FROM customers c\n"
    "LEFT JOIN orders o\n"
    "  ON c.customerNumber = o.customerNumber\n"
    "WHERE o.orderNumber IS NULL\n"
    "ORDER BY contactFirstName, contactLastName"
)
df_contacts = pd.read_sql(sql, conn)


# STEP 5: Payments sorted by correct amount
sql = (
    "SELECT TRIM(c.contactFirstName) AS contactFirstName, "
    "TRIM(c.contactLastName) AS contactLastName,\n"
    "       p.amount, p.paymentDate\n"
    "FROM customers c\n"
    "JOIN payments p\n"
    "  ON c.customerNumber = p.customerNumber\n"
    "ORDER BY CAST(p.amount AS REAL) DESC,\n"
    "         p.paymentDate DESC"
)
df_payment = pd.read_sql(sql, conn)


# STEP 6: Employees with avg credit limit > 90k
sql = (
    "SELECT e.employeeNumber, TRIM(e.firstName) AS firstName, "
    "TRIM(e.lastName) AS lastName,\n"
    "       COUNT(c.customerNumber) AS numCustomers\n"
    "FROM employees e\n"
    "JOIN customers c\n"
    "  ON e.employeeNumber = c.salesRepEmployeeNumber\n"
    "GROUP BY e.employeeNumber\n"
    "HAVING AVG(CAST(c.creditLimit AS REAL)) > 90000\n"
    "ORDER BY AVG(CAST(c.creditLimit AS REAL)) DESC,\n"
    "         e.employeeNumber"
)
df_credit = pd.read_sql(sql, conn)


# STEP 7: Product sales
sql = (
    "SELECT p.productName,\n"
    "       COUNT(DISTINCT od.orderNumber) AS numorders,\n"
    "       SUM(od.quantityOrdered) AS totalunits\n"
    "FROM products p\n"
    "JOIN orderdetails od\n"
    "  ON p.productCode = od.productCode\n"
    "GROUP BY p.productCode, p.productName\n"
    "ORDER BY totalunits DESC, numorders DESC,\n"
    "         p.productName"
)
df_product_sold = pd.read_sql(sql, conn)


# STEP 8: Number of customers per product
sql = (
    "SELECT p.productName, p.productCode,\n"
    "       COUNT(DISTINCT o.customerNumber) AS numpurchasers\n"
    "FROM products p\n"
    "JOIN orderdetails od\n"
    "  ON p.productCode = od.productCode\n"
    "JOIN orders o\n"
    "  ON od.orderNumber = o.orderNumber\n"
    "GROUP BY p.productCode, p.productName\n"
    "ORDER BY numpurchasers DESC, p.productName"
)
df_total_customers = pd.read_sql(sql, conn)


# STEP 9: Customers per office
sql = (
    "SELECT o.officeCode, o.city,\n"
    "       COUNT(DISTINCT c.customerNumber) AS n_customers\n"
    "FROM offices o\n"
    "LEFT JOIN employees e\n"
    "  ON o.officeCode = e.officeCode\n"
    "LEFT JOIN customers c\n"
    "  ON e.employeeNumber = c.salesRepEmployeeNumber\n"
    "GROUP BY o.officeCode, o.city\n"
    "ORDER BY n_customers DESC, o.officeCode"
)
df_customers = pd.read_sql(sql, conn)


# STEP 10: Employees who sold low-performing products (<20 customers)
sql = (
    "SELECT DISTINCT e.employeeNumber, "
    "TRIM(e.firstName) AS firstName, TRIM(e.lastName) AS lastName,\n"
    "       o.city, o.officeCode\n"
    "FROM employees e\n"
    "JOIN offices o ON e.officeCode = o.officeCode\n"
    "JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber\n"
    "JOIN orders ord ON c.customerNumber = ord.customerNumber\n"
    "JOIN orderdetails od ON ord.orderNumber = od.orderNumber\n"
    "WHERE od.productCode IN (\n"
    "    SELECT od2.productCode\n"
    "    FROM orderdetails od2\n"
    "    JOIN orders o2 ON od2.orderNumber = o2.orderNumber\n"
    "    GROUP BY od2.productCode\n"
    "    HAVING COUNT(DISTINCT o2.customerNumber) < 20\n"
    ")\n"
    "ORDER BY lastName, firstName"
)
df_under_20 = pd.read_sql(sql, conn)

print("STEP 1 - Boston Employees")
print(df_boston)

print("\nSTEP 2 - Offices with zero employees")
print(df_zero_emp)

print("\nSTEP 3 - Employees + Offices")
print(df_employee)

print("\nSTEP 4 - Customers with no orders")
print(df_contacts)

print("\nSTEP 5 - Payments")
print(df_payment)

print("\nSTEP 6 - High credit employees")
print(df_credit)

print("\nSTEP 7 - Product sales")
print(df_product_sold)

print("\nSTEP 8 - Customers per product")
print(df_total_customers)

print("\nSTEP 9 - Customers per office")
print(df_customers)

print("\nSTEP 10 - Employees (low products)")
print(df_under_20)

# Close connection
conn.close()
