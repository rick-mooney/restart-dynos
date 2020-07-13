import heroku3 as hk
import psycopg2 as pg
import os
from dotenv import load_dotenv
load_dotenv()

def connect():
    if os.getenv('TEST') == 'TRUE':
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


def restart(appname, key):
    conn = hk.from_key(key)
    app = conn.app(appname)
    dynos_restarted = 0
    for dyno in app.dynos():
        if dyno.type == 'web':
            dyno.restart()
            dynos_restarted += 1
    print('restarted ' + str(dynos_restarted) + ' dynos on app: ' + appname)

    # return app.restart() # restarts the whole dyno formation. returns a tuple of app name and key for some reason

def run():
    db = connect()
    columns = {
        "appname" : 0,
        "usertoken": 1
    }
    query = 'SELECT ' + ', '.join(columns.keys()) + ' FROM apps WHERE active = True'
    print(query)
    db.execute(query)
    apps = db.fetchall()
    for app in apps:
        print('restarting ' + app[columns['appname']] + '...')
        try:
            restart(app[columns['appname']], app[columns['usertoken']])
        except Exception as ex:
            # even if the dynos are off, its still a success. So this should just catch programming errors on my end.
            print('error restarting app: ' + app[columns['appname']])
            print(ex)
    print('all apps restarted.  Shutting down...')
    db.close()

if __name__ == '__main__':
    run()