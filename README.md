# internal-data-quality-sla-monitor

Prototype internal data quality & SLA monitoring platform for analytics tables (e‑commerce).

## 1. Project overview

This project behaves like a small internal data quality service for an analytics warehouse.  
Instead of trusting dashboards blindly, it runs automated checks on core tables (`orders`, `payments`) to verify volume, freshness, and basic data quality before stakeholders use the data.

It is inspired by modern data observability tools (Monte Carlo, Soda, etc.) but implemented with a simple, transparent stack suitable for a single‑developer portfolio project.

## 2. Tech stack

- **Database:** MySQL (`dq_sandbox` schema)  
- **Tables:** `orders`, `payments`, `dq_check_results`  
- **Checks & orchestration:** Python + `mysql-connector-python`  
- **Monitoring schema:** SQL table `dq_check_results` and view `dq_summary_table_run`  
- **Status dashboard:** Power BI (Data Quality & SLA Monitor – Status Dashboard)

## 3. What it monitors

Currently two core analytics tables:

- `orders`
  - Row count threshold (daily orders should not drop to 0 in this demo)
  - `customer_id` must not be null
  - Freshness: `MAX(updated_at)` must be within the last 1 day

- `payments`
  - Row count threshold (at least 1 payment row in this demo)
  - `payment_amount` must be non‑negative

Each check is evaluated as **PASS/FAIL** and logged to `dq_check_results` with:
`check_name`, `table_name`, `status`, `metric_value`, `expected_condition`, `run_time`.

## 4. How it works (high level)

1. **Python check runner**

   - Connects to MySQL (`dq_sandbox`) using a dedicated user.  
   - Runs SQL queries to compute metrics (row counts, null counts, max timestamps, negative amounts).  
   - Compares results to table‑level SLAs and classifies each check as PASS or FAIL.  
   - Inserts one row per check into `dq_check_results`.

2. **Monitoring view**

   - SQL view `dq_summary_table_run` aggregates pass/fail counts per `table_name` and `run_time`.  
   - This view is exported to CSV or read directly by Power BI.

3. **Power BI dashboard**

   - Shows total passed and failed checks as KPI cards.  
   - Displays a matrix of `table_name × run_time` with passed/failed counts.  
   - Includes a slicer to focus on one table (e.g., only `orders` or only `payments`).

## 5. Running the project

1. Create the `dq_sandbox` schema in MySQL and set up the `orders`, `payments`, and `dq_check_results` tables.  
2. Configure Python environment and install `mysql-connector-python`.  
3. Update connection details in `check_orders_rowcount.py` (host, user, password, database).  
4. Run the script to execute all checks and populate `dq_check_results`.  
5. Create the `dq_summary_table_run` view and either:
   - Export it to CSV and load into Power BI, or  
   - Connect Power BI directly to MySQL.  
6. Open `dq_sla_monitor.pbix` to view the status dashboard.

## 6. Future extensions

- Add more checks (distribution changes, daily volume anomaly rules).  
- Extend monitoring to additional tables (customers, transactions, etc.).  
- Schedule the Python script via cron/Airflow for fully automated daily runs.  
- Add email/Slack‑style alerts when critical checks fail.
