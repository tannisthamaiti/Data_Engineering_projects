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

import luigi
from luigi.s3 import S3Target, S3Client

class myTask(luigi.Task):
    def requires(self):
        return otherTask()

    def output(self):
        client = S3Client('ACCESS_KEY', 'SECRET_KEY')
        return S3Target('s3.amazonaws.com/mybucket/myfolder/myfile.tsv', client=client)

    def run(self):
         fo = self.output().open('w')
         with self.input().open('r') as f:
            data = dosomething_to_input(f)
            fo.write(data)
         fo.close()

