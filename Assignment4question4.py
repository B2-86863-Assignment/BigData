from pyhive import hive

# hive config
host_name = 'localhost'
port = 10000
user = 'aditya'
password = ' '
db_name = 'edbda'

# get hive connection
conn = hive.Connection(host=host_name, port=port, username=user, password=password, database=db_name, auth='CUSTOM')

# get the cursor object
cur = conn.cursor()

# execute the sql query using cursor
movieId = input('Enter The movieId: ')
sql = "select rc.movieid2, ms.title , rc.corr from mv_movie_corr rc INNER JOIN movie_staging ms ON ms.movieid=rc.movieid2  where rc.movieid1=%s ORDER BY rc.corr DESC LIMIT 5"
cur.execute(sql, [movieId])


result = cur.fetchall()
for row in result:
    print(row)

# close the connection
conn.close()
