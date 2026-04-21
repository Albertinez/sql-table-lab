# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)


# STEP 1: Employees in Boston
# FIX: removed jobTitle — test expects exactly 2 columns (firstName, lastName)
df_boston = pd.read_sql("""
SELECT e.firstName, e.lastName
FROM employees e
JOIN offices o
ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
""", conn)


# STEP 2: Offices with zero employees
df_zero_emp = pd.read_sql("""
SELECT o.officeCode, o.city
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
GROUP BY o.officeCode
HAVING COUNT(e.employeeNumber) = 0
""", conn)


# STEP 3: All employees + office info
df_employee = pd.read_sql("""
SELECT e.firstName, e.lastName, o.city, o.state
FROM employees e
LEFT JOIN offices o
ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName
""", conn)


# STEP 4: Customers with no orders
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o
ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactLastName
""", conn)


# STEP 5: Payments sorted by correct amount
df_payment = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
FROM customers c
JOIN payments p
ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC
""", conn)


# STEP 6: Employees with avg credit limit > 90k
# FIX: removed LIMIT 4 and changed ORDER BY so the correct employee (Loui) appears first
df_credit = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName,
       COUNT(c.customerNumber) AS numCustomers
FROM employees e
JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY AVG(c.creditLimit) DESC
""", conn)


# STEP 7: Product sales
df_product_sold = pd.read_sql("""
SELECT p.productName,
       COUNT(od.orderNumber) AS numorders,
       SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od
ON p.productCode = od.productCode
GROUP BY p.productCode
ORDER BY totalunits DESC
""", conn)


# STEP 8: Number of customers per product
df_total_customers = pd.read_sql("""
SELECT p.productName, p.productCode,
       COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od
ON p.productCode = od.productCode
JOIN orders o
ON od.orderNumber = o.orderNumber
GROUP BY p.productCode
ORDER BY numpurchasers DESC
""", conn)


# STEP 9: Customers per office
df_customers = pd.read_sql("""
SELECT o.officeCode, o.city,
       COUNT(c.customerNumber) AS n_customers
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
LEFT JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode
""", conn)


# STEP 10: Employees who sold low-performing products (<20 customers)
df_under_20 = pd.read_sql("""
SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName,
       o.city, o.officeCode
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders ord ON c.customerNumber = ord.customerNumber
JOIN orderdetails od ON ord.orderNumber = od.orderNumber
WHERE od.productCode IN (
    SELECT od.productCode
    FROM orderdetails od
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY od.productCode
    HAVING COUNT(DISTINCT o.customerNumber) < 20
)
""", conn)

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
