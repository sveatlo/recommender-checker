extern crate clap;

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, prelude::*, BufReader};

use clap::{App, Arg, SubCommand};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("mRecommender checker")
        .about("Prepares data for mRecommender and validates mRecommender decisions")
        .subcommand(
            SubCommand::with_name("prepare")
                .about("Prepares a csv data dump from DB by aggregating the shows per user and converting show IDs from string to int")
                .arg(Arg::with_name("in-file")
                        .help("Input csv file without header with format: user,show,created_at"))
                .arg(Arg::with_name("out-file")
                        .help("Output processed file")
                        .default_value("./mrecommender.dataset"))
        )
        .subcommand(
            SubCommand::with_name("validate")
                .about("Performs the cross-validation")
                .arg(Arg::with_name("address")
                        .long("host")
                        .short("h")
                        .help("Remote address to which the request should be send")
                        .default_value("localhost:3000"))
                .arg(Arg::with_name("dataset-file")
                        .help("Prepared dataset file"))
        )
            .get_matches();

    match matches.subcommand() {
        ("prepare", Some(sub_m)) => {
            let input_file_path = sub_m.value_of("in-file").unwrap();
            let output_file_path = sub_m.value_of("out-file").unwrap();

            let f = File::open(input_file_path)?;
            let mut fo = File::create(output_file_path)?;

            prepare_dataset(&f, &mut fo)
        }
        ("validate", Some(sub_m)) => {
            let address = sub_m.value_of("address").unwrap();
            let dataset_file_path = sub_m.value_of("dataset-file").unwrap();
            let f = File::open(dataset_file_path)?;

            validate(address, f).await
        }
        _ => Err(Box::from(SimpleError::new("invalid command"))),
    }
}

fn prepare_dataset(in_file: &File, out: &mut File) -> Result<(), Box<dyn Error>> {
    let reader = BufReader::new(in_file);

    let mut dataset: HashMap<i64, Vec<i64>> = HashMap::new();

    for (i, liner) in reader.lines().enumerate() {
        let line = liner?;

        let cols: Vec<&str> = line.split(',').collect();

        let ui_col = cols[0].trim_matches('"');
        let si_col = cols[1].trim_matches('"');
        // let ca_col = cols[2].trim_matches('"');

        if ui_col == "<nil>" || si_col == "<nil>" {
            continue;
        }

        let ui = ui_col.parse::<i64>().unwrap_or_else(|e| {
            panic!(format!("invalid user id `{}` on line {}: {}", ui_col, i, e));
        });
        let si = si_col.parse::<f64>().unwrap_or_else(|e| {
            panic!(format!("invalid show id `{}` on line {}: {}", si_col, i, e));
        });
        // let ca = DateTime::parse_from_str(ca_col, "%Y-%m-%d %H:%M:%S")
        //     .unwrap_or_else(|e| panic!(format!("invalid date `{}` on line {}", si_col, i)));
        //

        let user_shows = dataset.entry(ui).or_insert_with(Vec::new);
        user_shows.push(si as i64)

        // println!("{},{},{}", ui, si, ca)
    }

    for (user, shows) in &dataset {
        if shows.len() < 25 {
            continue;
        }

        let mut ss: Vec<String> = shows.iter().map(|&x| format!("{}", x)).collect();
        ss.dedup();
        write!(out, "{}: {}\n", user, ss.join(","))?;
    }

    // serde_yaml::to_writer(out, &dataset)?;

    Ok(())
}

async fn validate(address: &str, f: File) -> Result<(), Box<dyn Error>> {
    let reader = BufReader::new(f);

    let client = reqwest::Client::new();

    let mut avg_src = 0f64;
    let mut ln = 0;
    for liner in reader.lines() {
        ln += 1;

        let line = liner?;
        let lv: Vec<&str> = line.split(':').map(|x| x.trim()).collect();
        let shows: Vec<i64> = lv[1]
            .split(',')
            .map(|x| x.parse::<i64>().unwrap())
            .collect();

        let d = ((shows.len() as f64) * 0.8) as usize;
        let train = shows.get(0..d).unwrap();
        let test = shows.get(d..).unwrap();
        let mut test_map: HashMap<i64, bool> = HashMap::new();
        for t in test {
            test_map.insert(*t, true);
        }

        let addr = format!("http://{}/test_recommendation", address);
        let mut recommended = client
            // .post("http://localhost:3000/test_recommendation")
            // .post("http://10.10.1.148:9001/test_recommendation")
            .post(&*addr)
            .json(&train)
            .send()
            .await?
            .json::<Vec<i64>>()
            .await?;

        recommended.sort_unstable();
        recommended.dedup();

        let mut n = 0;
        for r in recommended {
            if test_map.contains_key(&r) {
                n += 1;
            }
        }

        avg_src += (n as f64) / (test.len() as f64);
        println!(
            "{} of {} => {}\n",
            n as f64,
            test.len() as f64,
            n as f64 / (test.len() as f64)
        );
    }
    println!("{}%", avg_src / (ln as f64) * 100f64);

    Ok(())
}

#[derive(Debug)]
struct SimpleError {
    msg: String,
}

impl SimpleError {
    fn new(msg: &str) -> SimpleError {
        SimpleError {
            msg: msg.to_string(),
        }
    }
}

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error for SimpleError {
    fn description(&self) -> &str {
        &self.msg
    }
}
