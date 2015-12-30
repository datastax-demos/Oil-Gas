CREATE KEYSPACE IF NOT EXISTS oilgas WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'SearchAnalytics': '1'
};

CREATE TABLE IF NOT EXISTS oilgas.drill_data (
	drill_id uuid,
    td decimal,
    bittd decimal,
    wob decimal,
    rpm decimal,
    rop decimal,
    flowin decimal,
    flowout decimal,
    plcirc decimal,
    mwin decimal,
    mwout decimal,
	data_timestamp timestamp,
	date text,
    PRIMARY KEY ((drill_id,date), data_timestamp)
) WITH CLUSTERING ORDER BY (data_timestamp DESC);

CREATE TABLE IF NOT EXISTS oilgas.drills (
	drill_id int,
    drill_name text,
    PRIMARY KEY (drill_id)
);
