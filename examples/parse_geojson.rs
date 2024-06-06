use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use clap::Parser;
use serde::{Deserialize, Deserializer, Serialize};
use serde::de::Error;

use keplerize::{Data, Dataset, Feature, Info, LineString, Row};

// source mf json, converting this to geojson
#[derive(Deserialize, Debug)]
struct Rec {
    pub id: u64,
    pub vt: u32,
    pub json: Mf,
}

#[derive(Deserialize, Debug)]
struct Mf {
    pub coordinates: Vec<[f64; 2]>,
}

#[derive(Deserialize, Serialize, Debug)]
struct MyRow(Feature<LineString>, u64, u32);

#[typetag::serde]
impl Row for MyRow {}

impl From<Rec> for MyRow {
    fn from(src: Rec) -> Self {
        let coords = src.json.coordinates.into_iter().map(|([x, y])| [x, y, 0.0]);
        let g = LineString {
            //geometry_type: "LineString",
            coordinates: coords.map(|x| x.into()).collect(),
        };
        MyRow(Feature { geometry: g }, src.id, src.vt)
    }
}

/// Parse mf-json into a kepler.gl line dataset
#[derive(Clone, Debug, Parser)]
struct Opts {
    /// Path to the input mf-json
    input: String,

    /// Path to the output json
    output: String,

    /// Unique id of the dataset, optional
    #[clap(long, default_value = "my-dataset")]
    id: String,

    /// Unique id of the dataset, optional
    #[clap(long, default_value = "My Dataset")]
    label: String,

    /// Maximum number of records to write
    #[clap(short, long)]
    limit: Option<usize>,

    /// Filter out trips with less than this number posits
    #[clap(long, default_value = "1")]
    min_trip_size: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();

    let input = File::open(opts.input)
        .map(BufReader::new)
        .expect("open input");

    let output = File::options()
        .create(true)
        .write(true)
        .open(opts.output)
        .expect("open output");

    let lines: Result<Vec<_>, _> = input.lines().collect();
    let rows: Vec<_> = lines?
        .into_iter()
        .flat_map(|s| serde_json::from_str::<Rec>(&s))
        .map(MyRow::from)
        .filter(|r| r.0.geometry.coordinates.len() >= opts.min_trip_size)
        .collect();

    let rows = if let Some(limit) = opts.limit {
        rows.into_iter().take(limit).collect()
    } else {
        rows
    };

    let ds = Dataset::<MyRow> {
        info: Info {
            id: &opts.id,
            label: &opts.label,
        },
        data: Data {
            fields: &["id".into(), "vessel-type".into()],
            rows: &rows,
        },
    };

    let serialized = serde_json::to_string(&ds).unwrap();
    writeln!(&output, "{}", &serialized)?;

    Ok(())
}
