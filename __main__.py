import sys, os, datetime
try:
	import psycopg2, pandas
except ImportError:
	print('Module requires Pandas & pysopg2')
	sys.exit(0)

hostname = os.environ.get('RS_HOSTNAME')
defaultdb = os.environ.get('RS_DEFAULTDB')
username = os.environ.get('RS_USER')
pw = os.environ.get('RS_PASS')
port=5439

if not username or not pw or not hostname or not defaultdb:
	print('Username and PW not\n' +\
		   'Set using "export RS_USER=[username] and export RS_PASS=[pw]')

conn = psycopg2.connect(host=hostname,database=defaultdb,user=username,port=port,password=pw)


def qdf(query_string):
    '''Executes query against Redshift'''
    a = datetime.datetime.now()
    c = conn.cursor()
    try:
        c.execute(query_string)
    except conn.InternalError as e: # Roll back connection if previous query errored out
        print 'Warning, internal error. Automatically rolling back and retrying...\nFull error msg: %s' % (e)
        conn.rollback()
        c.execute(query_string)


    b = datetime.datetime.now()
    print 'Total runtime: %d seconds' % ((b-a).total_seconds())
    df = pandas.DataFrame(c.fetchall())
    conn.rollback()
    if not df.empty:
        df.columns = [x[0] for x in c.description]
        return df

    else:
        return 'Empty Frame'

