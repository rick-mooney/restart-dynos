import heroku3 as hk
import psycopg2 as pg
from psycopg2 import extras as pg_extras
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json
import csv
load_dotenv()

# legacy, previously was not using dict cursor.  
# Column names are still referenced, but doesn't 
# necessarily need to be a dict any more
columns = {
    "appname" : 0,
    "usertoken": 1,
    "last_restart": 2,
    "frequency": 3,
    "next_run" : 4
}

def connect():
    '''
        Manages database connection 
    '''
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

def fetch_app_config(key):
    '''
     Retrieve dyno formation info for all apps that an api key has access to
    '''
    results = {}
    conn = hk.from_key(key)
    apps = conn.apps()
    for app in apps:
        dynos = app.dynos()
        for dyno in dynos:
            results.setdefault(app.name, []).append({dyno.name : 
                                                    {"type": dyno.type,
                                                    "size": dyno.size,
                                                    "state": dyno.state}
                                                })
    return results

def describe_apps():
    '''
    Parent function to get app config for all customers
    '''
    db = connect()
    cur = db.cursor(cursor_factory=pg_extras.DictCursor)
    query = "SELECT DISTINCT usertoken FROM apps where company != 'grax'"
    cur.execute(query)
    apps = cur.fetchall()
    results = {}
    for app in apps:
        results[app['usertoken']] = fetch_app_config(app['usertoken'])
    with open('results.json', 'w') as f:
        json.dump(results, f)
    print('describe apps complete')

def convert_describe_to_csv():
    '''
    One off script converting app describe info to csv format
    '''
    headers = ['id', 'appname', 'dyno', 'type', 'size', 'state']
    outfile = open('results.csv', 'w')
    csv_writer = csv.writer(outfile)
    csv_writer.writerow(headers)
    with open('results.json') as file:
        data = json.load(file)
    for key, apps in data.items():
        for app, info in apps.items():
            for dyno in info:
                for d_type, detail in dyno.items():
                    csv_writer.writerow([key, app, dyno, d_type, 
                                        detail['type'], detail['size'], 
                                        detail['state']])
    outfile.close()

def remove_all_drains(appname, key):
    '''
        Removes all drains for a given app
    '''
    try:
        conn = hk.from_key(key)
        app = conn.app(appname)
        logdrainlist = app.logdrains()
        print('found ' + str(len(logdrainlist)) + ' log drains')

        for drain in logdrainlist:
            print(app.remove_logdrain(drain.id))
    except Exception as ex:
        print(f'warning: error connecting to app: {ex}')

def add_drain(appname, key):
    '''
        adds grax papertrail drain to given app
    '''
    print(f'adding drain for {appname}')
    drain = os.environ.get('DRAIN_URL')
    conn = hk.from_key(key)
    app = conn.app(appname)
    log = app.create_logdrain(drain)
    print(log.token)


def restart(appname, key):
    '''
        Makes the call to restart the web dyno on a given app
    '''
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
    '''
        Process checks db for apps where next run time is in the past
        and restarts the web dyno
    '''
    db = connect()
    cur = db.cursor(cursor_factory=pg_extras.DictCursor)
    query = 'SELECT ' + ', '.join(columns.keys()) + ' FROM apps WHERE active = True and next_run <= NOW()'
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

def manage_drains(method, filter=None):
    db = connect()
    cur = db.cursor(cursor_factory=pg_extras.DictCursor)
    query = 'SELECT appname, usertoken FROM apps'
    cur.execute(query)
    apps = cur.fetchall()
    for app in apps:
        if method == 'remove':
            remove_all_drains(app['appname'], app['usertoken'])
        elif method == 'add':
            add_drain(app['appname'], app['usertoken'])
        else:
            print('method not supported')

def add_drain_by_appname(apps):
    db = connect()
    cur = db.cursor(cursor_factory=pg_extras.DictCursor)
    query = f'SELECT appname, usertoken FROM apps WHERE appname in {apps}'
    cur.execute(query)
    apps = cur.fetchall()
    for app in apps:
        add_drain(app['appname'], app['usertoken'])
        # conn = hk.from_key(app['usertoken'])
        # thisapp = conn.app(app['appname'])
        # logs = thisapp.logdrains()
        # for log in logs:
        #     print('log id for ' + app['appname'])
        #     print(log.token)
def generate_papertrail_url():
    db = connect()
    cur = db.cursor(cursor_factory=pg_extras.DictCursor)
    query = f'SELECT appname FROM apps WHERE owneremail is null'
    cur.execute(query)
    apps = cur.fetchall()
    for app in apps:
        appname = app['appname']
        print(f'https://papertrailapp.com/systems/{appname}/edit')

if __name__ == '__main__':
    # run your code
    pass