# Data Dictionary For Gold Layer

## Overview
The gold layer is the business level data representation, structured to support analysis and reporting. This layer consists of **fact tables** and **dimension tables** for specific business metrics.

---

### 1. **gold.dim_customers**
- **Purpose**: Stores customer details enriched with geographical and demographic data
- **Columns:**

| Columns         | Data Type             | Description                                                                                |
------------------|-----------------------|--------------------------------------------------------------------------------------------|
| customer_key    | bigint                | Surrogate key that identifies each customer record uniquely in the table.                  |
| customer_id     | integer               | Unique numerical identifier assigned to each customer.                                     |
| customer_number | character varying(50) | Unique alphanumeric identifier assigned to each customer, used for tracking and reference. |
| firstname       | character varying(50) | The first name of the customer.                                                            |
| lastname        | character varying(50) | The last name of the customer.                                                             |
| country         | character varying(50) | The country of residence for the customer (e.g. Germany).                                  |
| marital_status  | character varying(50) | The marital status of the customer (e.g. 'Single', 'Married').                             |
| gender          | character varying(50) | The gender of the customer (e.g. 'Male', 'Female', 'n/a').                                 |
| birthdate       | date                  | The birth date of the customer formatted as YYYY-MM-DD (e.g. 1981-08-21).                  |
| create_date     | date                  | The date the customer was recording into the system.                                       |

---

### 2. **gold.dim_products**
- **Purpose**: List products and their attributes including various categorical details
- **Columns:**

| Columns         | Data Type             | Description                                                                                           |
------------------|-----------------------|-------------------------------------------------------------------------------------------------------|
| product_key    | bigint                | Surrogate key that identifies each product record uniquely in the table.                               |
| product_id     | integer               | Unique numerical identifier assigned to each product.                                                  |
| product_number | character varying(50) | Unique alphanumeric identifier assigned to each product, encoded with categorical details.             |
| product_name   | character varying(50) | A descriptive name of the product that includes details such as color and category.                    |
| category_id    | character varying(50) | A short, unique code for the each of the product categories, linking to its high-level classification. |
| category       | character varying(50) | A broader classificaiton of a product's category (e.g. Bikes, Components) used to group related items. |
| subcategory    | character varying(50) | A more detailed category for the product that is within a product's category.                          |
| maintenance    | character varying(50) | Indicates whether a product requires maintenance (e.g. 'Yes', 'No').                                   |
| cost           | integer               | The cost or base price of the product.                                                                 |
| product_line   | character varying(50) | The line or specific series to which the product belongs to (e.g. 'Mountain', 'Road')                  |
| start_date     | date                  | The date the product was made available for selling.                                                   |

---

### 3. **gold.fact_sales**
- **Purpose**: Stores transactional sales data for analytical purposes.
- **Columns:**

| Columns      | Data Type             | Description                                                                                |
---------------|-----------------------|--------------------------------------------------------------------------------------------|
| order_number | character varying(50) | A unique alphanumeric identifier for each sales order .                                    |
| product_key     | INT           | Surrogate key linking the order to the product dimension table.                               |
| customer_key    | INT           | Surrogate key linking the order to the customer dimension table.                              |
| order_date      | DATE          | The date when the order was placed.                                                           |
| shipping_date   | DATE          | The date when the order was shipped to the customer.                                          |
| due_date        | DATE          | The date when the order payment was due.                                                      |
| sales_amount    | INT           | The total monetary value of the sale for the line item, in whole currency units (e.g., 25).   |
| quantity        | INT           | The number of units of the product ordered for the line item (e.g., 1).                       |
| price           | INT           | The price per unit of the product for the line item, in whole currency units (e.g., 25).|
