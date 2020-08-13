import heroku3 as hk
import psycopg2 as pg
from psycopg2 import extras as pg_extras
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
load_dotenv()

# legacy, previously was not using dict cursor
columns = {
    "appname" : 0,
    "usertoken": 1,
    "last_restart": 2,
    "frequency": 3,
    "next_run" : 4
}

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

    return db


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
    cur = db.cursor(cursor_factory=pg_extras.DictCursor)
    query = 'SELECT ' + ', '.join(columns.keys()) + ' FROM apps WHERE active = True and next_run <= NOW()'
    print(query)
    cur.execute(query)
    apps = cur.fetchall()
    restarted_apps = []
    for app in apps:
        print('restarting ' + app['appname'] + '...')
        try:
            restart(app['appname'], app['usertoken'])
            restarted_apps.append((app['appname'], datetime.now(), datetime.now() + timedelta(hours=app['frequency'])))
        except Exception as ex:
            # even if the dynos are off, its still a success. So this should just catch programming errors on my end.
            print('error restarting app: ' + app['appname'])
            print(ex)

    # update app next run times
    update_statement = """ UPDATE apps SET last_restart = data.restarted,next_run = data.next FROM (VALUES %s) AS data (name, restarted, next) WHERE appname = data.name"""
    print(update_statement)
    pg_extras.execute_values(cur, update_statement, restarted_apps)
    db.commit()
    print('all apps restarted.  Shutting down...')
    cur.close()

if __name__ == '__main__':
    run()