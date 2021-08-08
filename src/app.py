from src.database import Database
from src.sql import CloudSQL
from secret.config import config


class App:
    def __init__(self):
        self.db = Database()
        self.sql = CloudSQL(config['FOLDER_PATH'])

    def solve(self):
        try:
            self.db.connect()
            self.sql.load_customers_to_db('customers.csv', self.db.cur)
            self.sql.load_orders_to_db('orders.csv', self.db.cur)
            self.sql.cross_check(self.db.cur)
            self.db.close()
        except Exception as err:
            print(err)



