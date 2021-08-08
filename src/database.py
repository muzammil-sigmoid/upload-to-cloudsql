import psycopg2
from secret.config import config


class Database:

    def __init__(self):
        self.uri = f"dbname={config['DBNAME']} user={config['USER']} password={config['PASSWORD']} host={config['HOST']}"
        self.cur = None
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.uri)
            self.cur = self.conn.cursor()
        except Exception as err:
            print(err)
            raise Exception("Database Connection Failed.")

    def close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()










