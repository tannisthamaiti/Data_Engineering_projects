dbname = 'dev'
port = '5439'
user = 'titliaws'
password = ''
aws_access_key_id = ''
aws_secret_access_key = ''
host = 'redshift-cluster-1.****.us-west-2.redshift.amazonaws.com'
host1 = 'aws_iam_role=arn:aws:iam::****:role/RedshiftCopyUnload' #myRedshiftRole

import psycopg2
#Amazon Redshift connect string
conn_string = "dbname='dev' port= '5439' user= 'titliaws' password= '' \
            host= 'training007.*****.us-west-2.redshift.amazonaws.com'"
con = psycopg2.connect(conn_string);
#to_table = 'users'
to_table = 'tweets'
#fn = 's3://awssampledbuswest2/tickit/allusers_pipe.txt'
fn = 's3://kaggletest/twitter_loads/twitter.csv'
delim = '|'
sql="""COPY %s FROM '%s' credentials '%s'
       delimiter '%s' region 'us-west-2';""" % (to_table, fn, host1, delim)
cur = con.cursor()
cur.execute(sql)
cur.execute("commit;")
print("Copy executed fine!")



