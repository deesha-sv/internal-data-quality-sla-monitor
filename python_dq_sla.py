import mysql.connector
from mysql.connector import Error

def run_rowcount_check():
    try:
        # 1. Connect to MySQL
        connection = mysql.connector.connect(
            host="localhost",
            user= "dq_user",                 
            password="StrongPass123!",  
            database="dq_sandbox"        
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # 2. Run a simple data-quality query
            cursor.execute("SELECT COUNT(*) FROM orders;")
            result = cursor.fetchone()
            total_orders = result[0]

            # 3. Very simple SLA rule for now
            expected_min_orders = 1

            status = "PASS" if total_orders >= expected_min_orders else "FAIL"
            print(f"Row count check on 'orders': {status}")
            print(f"Total orders = {total_orders}, expected >= {expected_min_orders}")

                        # Save row count check result
            insert_sql = """
                INSERT INTO dq_check_results
                    (check_name, table_name, status, metric_value, expected_condition)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                "orders_rowcount_min_1",
                "orders",
                status,
                float(total_orders),
                "total_orders >= 1"
            ))
            connection.commit()


            # --- Second check: customer_id nulls ---
            cursor.execute("SELECT COUNT(*) FROM orders WHERE customer_id IS NULL;")
            null_customer_ids = cursor.fetchone()[0]

            if null_customer_ids == 0:
                status_null = "PASS"
            else:
                status_null = "FAIL"

            print(f"Null check on orders.customer_id: {status_null}")
            print(f"Null customer_id rows = {null_customer_ids}, expected = 0")

                        # --- Third check: freshness on updated_at ---
            cursor.execute("SELECT MAX(updated_at) FROM orders;")
            latest_update = cursor.fetchone()[0]

            # consider data fresh if updated within last 1 day
            import datetime
            now = datetime.datetime.now()
            max_allowed_age = datetime.timedelta(days=1)

            if latest_update is not None and (now - latest_update) <= max_allowed_age:
                status_fresh = "PASS"
            else:
                status_fresh = "FAIL"

            print(f"Freshness check on orders.updated_at: {status_fresh}")
            print(f"Latest update = {latest_update}, now = {now}")

            # Save freshness check result
            cursor.execute(insert_sql, (
                "orders_fresh_within_1_day",
                "orders",
                status_fresh,
                0.0,  # or (now - latest_update).total_seconds() if you want numeric
                "MAX(updated_at) within 1 day"
            ))
            connection.commit()

                        # --- PAYMENTS CHECKS ---

            # 1) Row count check for payments
            cursor.execute("SELECT COUNT(*) FROM payments;")
            payments_count = cursor.fetchone()[0]
            expected_min_payments = 1
            status_payments_rowcount = "PASS" if payments_count >= expected_min_payments else "FAIL"

            print(f"Row count check on 'payments': {status_payments_rowcount}")
            print(f"Total payments = {payments_count}, expected >= {expected_min_payments}")

            # Save payments row count result
            cursor.execute(insert_sql, (
                "payments_rowcount_min_1",
                "payments",
                status_payments_rowcount,
                float(payments_count),
                "total_payments >= 1"
            ))
            connection.commit()

            # 2) Non-negative payment_amount check
            cursor.execute("SELECT COUNT(*) FROM payments WHERE payment_amount < 0;")
            negative_payments = cursor.fetchone()[0]
            status_payments_amount = "PASS" if negative_payments == 0 else "FAIL"

            print(f"payment_amount >= 0 check on 'payments': {status_payments_amount}")
            print(f"Rows with negative payment_amount = {negative_payments}, expected = 0")

            # Save payments amount check result
            cursor.execute(insert_sql, (
                "payments_amount_non_negative",
                "payments",
                status_payments_amount,
                float(negative_payments),
                "negative_payment_amounts = 0"
            ))
            connection.commit()



            # close cursor here
            cursor.close()

    except Error as e:
        print("Error while connecting to MySQL or running query:", e)

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    run_rowcount_check()


