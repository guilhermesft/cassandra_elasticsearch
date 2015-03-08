CREATE KEYSPACE IF NOT EXISTS simbiose WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3 };
CREATE COLUMNFAMILY IF NOT EXISTS simbiose.tweets ( id uuid PRIMARY KEY , user text , content text , retweet int, date timestamp , likes int, last_update timestamp );
