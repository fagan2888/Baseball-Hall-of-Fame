# File: example_db_use.py
# Date Created: 2018-10-20
# Author(s): Mahkah Wu
# Purpose: Example usage of Postgres database with Python

import psycopg2
import pandas as pd

# Import database credentials from file ignored by GitHub and set up connection
from ignore import db_cred
conn = db_cred.connect_db()

# SQL to retrieve hall of fame data about the five players who have been considered for the hall of fame the most times
query = '''
select * from hall_of_fame
where hall_of_fame."playerID" in (
    select "playerID" from (
        select count(m."playerID") as hof_rounds, avg(votes) as average_votes, m."playerID" from master as m
        inner join hall_of_fame as hof
        on m."playerID" = hof."playerID"
        group by m."playerID"
        order by hof_rounds desc
        limit 5
    ) as t1
)
order by "playerID", yearid;
'''

# Option 1: Use psycopg2 directly
cur = conn.cursor()
cur.execute(query)
cur.fetchone()

# Option 2: Use pandas read_sql
df = pd.read_sql(query, conn)
df.head()
