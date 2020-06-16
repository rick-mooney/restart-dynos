import heroku3 as hk
import psycopg2 as pg
import os
from dotenv import load_dotenv
load_dotenv()

def connect():
    if os.getenv('TEST'):
        db = pg.connect(user=os.getenv('DB_USER'),
                        password=os.getenv('DB_PASSWORD'),
                        host=os.getenv('DB_HOST'),
                        port=os.getenv('DB_PORT'),
                        database=os.getenv('DB_NAME')
            )
    else:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        db = pg.connect(DATABASE_URL, sslmode='require')

    return db.cursor()


def restart(app, key):
    conn = hk.from_key(key)
    app = conn.app(app)
    res = app.restart()
    print(res)

def run():
    db = connect()
    db.execute('SELECT appname, key from apps where active = True')
    apps = db.fetchAll()
    for app in apps:
        restart(app.appname, app.key)
    db.close()

if __name__ == '__main__':
    run()