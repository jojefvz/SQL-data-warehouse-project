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
