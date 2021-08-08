import pandas
import os

class CloudSQL:

    def __init__(self, folder_path):
        self.FOLDER_PATH = folder_path

    def get_formatted_string(self, word):
        word = str(word)
        parts = word.split("\'")
        return "\'\'".join(parts)

    def load_customers_to_db(self, file_name, cur):
        try:
            self.df_customers = pandas.read_csv(os.path.join(self.FOLDER_PATH, file_name))
            cur.execute(f"drop table if exists customers;")
            #cur.execute(f"alter role postgres with superuser")
            cur.execute(f"create table customers ("
                        f"CustomerID numeric,"
                        f"CustomerName varchar(256),"
                        f"ContactName varchar(256),"
                        f"Address varchar,"
                        f"City varchar(256),"
                        f"PostalCode varchar,"
                        f"Country varchar(256));")
            for row in self.df_customers.to_records(index=False):
                cur.execute(f"insert into customers values ({row[0]},'{self.get_formatted_string(row[1])}','{self.get_formatted_string(row[2])}',"
                            f"'{self.get_formatted_string(row[3])}','{self.get_formatted_string(row[4])}','{self.get_formatted_string(row[5])}', '{row[6]}');")
            #cur.execute('select * from customers')
        except Exception as err:
            print(err)
            raise Exception("Table load failed for customers table")

    def load_orders_to_db(self,file_name, cur):
        try:
            self.df_orders = pandas.read_csv(os.path.join(self.FOLDER_PATH, file_name))
            cur.execute(f"drop table if exists orders;")
            cur.execute(f"create table orders ("
                        f"OrderID numeric,"
                        f"CustomerID numeric,"
                        f"EmployeeID numeric,"
                        f"OrderDate date,"
                        f"ShipperID numeric"
                        f");")
            for row in self.df_orders.to_records(index=False):
                cur.execute(f"insert into orders values {row}")
            cur.execute('select * from orders;')
        except Exception as err:
            print(err)
            raise Exception("loading orders failed")

    def cross_check_helper(self,df, records):
        for row in df.to_records(index=False):
            print(row[0])
            print(records[0])
            if row[0] != records[0]:
                raise Exception("Data inconsistency..")

    def cross_check(self, cur):
        try:
            cur.execute('select OrderID from orders;')
            order_records = cur.fetchall()
            cur.execute('select CustomerID from customers;')
            customer_records = cur.fetchall()
            self.cross_check_helper(self.df_orders,order_records)
            self.cross_check_helper(self.df_customers,customer_records)



        except Exception as err:
            print(err)
            raise Exception("Cross check Failed")










