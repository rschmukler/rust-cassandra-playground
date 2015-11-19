extern crate time;
extern crate cassandra;
use cassandra::*;
use std::env;

static CREATE_KEYSPACE:&'static str = "
    CREATE KEYSPACE IF NOT EXISTS benchmark WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 2 }";

static CREATE_TABLE:&'static str = "
    CREATE TABLE IF NOT EXISTS benchmark.simple (
      id int PRIMARY KEY,
      count int
    )";

static INSERT_QUERY:&'static str = "INSERT INTO benchmark.simple \
     (id, count) VALUES (1, 1)";

#[derive(Debug, Clone, Copy)]
struct Record {
    id: i32,
    field_one: &'static str,
    is_person: &'static str,
    count: i32,
    some_other_field: &'static str,
    another: i32,
    is_frank: f32,
    likes_cats: f32,
}

impl Record {
    fn insert(&self, session: &mut Session) {
        // let prepared = session.prepare(INSERT_QUERY).unwrap().wait().unwrap();
        // let mut statement = prepared.bind();

        let mut statement = Statement::new(INSERT_QUERY, 2);
        statement
            .bind_int32(0, self.id).unwrap()
            .bind_int32(0, self.count).unwrap();
        
            // .bind_string(1, self.field_one).unwrap()
            // .bind_string(2, self.is_person).unwrap()
            // .bind_int32(3, self.count as i32).unwrap()
            // .bind_string(4, self.some_other_field).unwrap()
            // .bind_int32(5, self.another as i32).unwrap()
            // .bind_float(6, self.is_frank).unwrap()
            // .bind_float(7, self.likes_cats).unwrap();

        session.execute(INSERT_QUERY, 0).wait().unwrap();
    }
}


fn main() {
    if let Some(benchmark_string) = env::args().nth(1) {
        let benchmark_count = benchmark_string.parse::<u32>().unwrap();

        let records = make_sample(benchmark_count as usize);

        let mut cluster = Cluster::new();
        cluster.set_contact_points("cassandra").unwrap();
        cluster.set_protocol_version(3).unwrap();

        let mut session = cluster.connect().unwrap();
        session.execute(CREATE_KEYSPACE, 0).wait().unwrap();
        session.execute(CREATE_TABLE, 0).wait().unwrap();


        let start_time = time::precise_time_ns() as f64;
        for record in records {
            record.insert(&mut session);
        }

        session.close().wait().unwrap();

        let end_time = time::precise_time_ns() as f64;
        let total_time_ms = (end_time - start_time) / 1000000 as f64;
        let rps = benchmark_count as f64 / total_time_ms * 1000 as f64;

        println!("Inserted {} recs in {:.*}ms ({:.*} recs/s)", benchmark_count, 2, total_time_ms, 2, rps);
    } else {
        println!("Please enter a number of records to insert...");
    }
}

fn make_sample(size: usize) -> Vec<Record> {
    let mut records: Vec<Record> = Vec::with_capacity(size);

    for i in 0..size {
        records.push(Record{
            id: i as i32,
            field_one: "blah",
            is_person: "hello-fuck",
            count: 25,
            some_other_field: "hello-this-is a test of something else then",
            another: 100,
            is_frank: 0.1,
            likes_cats: 0.51,
        })
    }

    return records;
}


