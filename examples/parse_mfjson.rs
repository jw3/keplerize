use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use chrono::DateTime;
use serde::{Deserialize, Deserializer, Serialize};
use serde::de::Error;

use keplerize::{Data, Dataset, Feature, Info, LineString, Row};

#[derive(Deserialize, Debug)]
struct Rec {
    pub id: u64,
    pub json: Mf,
}

#[derive(Deserialize, Debug)]
struct Mf {
    pub coordinates: Vec<[f64; 2]>,

    #[serde(deserialize_with = "str_to_ts")]
    pub datetimes: Vec<i64>,
}

fn str_to_ts<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<i64>, D::Error> {
    let s: Vec<String> = Deserialize::deserialize(d)?;
    let r: Vec<_> = s
        .iter()
        .flat_map(|x| DateTime::parse_from_str(x, "%Y-%m-%dT%T%#z"))
        .map(|x| x.timestamp())
        .collect();

    if s.len() == r.len() {
        Ok(r)
    } else {
        Err(Error::custom(format!(
            "lossy ts convert: {} to {}",
            s.len(),
            r.len()
        )))
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct MyRow(Feature, u64);

#[typetag::serde]
impl Row for MyRow {}

impl From<Rec> for MyRow {
    fn from(src: Rec) -> Self {
        assert_eq!(src.json.coordinates.len(), src.json.datetimes.len());
        let coords = src
            .json
            .datetimes
            .into_iter()
            .map(|t| t as f64)
            .zip(src.json.coordinates)
            .into_iter()
            .map(|(t, [x, y])| [x, y, 0.0, t]);
        let g = LineString {
            //geometry_type: "LineString",
            coordinates: coords.collect(),
        };
        MyRow(Feature { geometry: g }, src.id)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let buf = File::open("/tmp/out-json.txt").map(BufReader::new)?;
    let lines: Result<Vec<_>, _> = buf.lines().collect();
    let rows: Vec<_> = lines?
        .into_iter()
        .flat_map(|s| serde_json::from_str::<Rec>(&s))
        .map(MyRow::from)
        .collect();
    let ds = Dataset::<MyRow> {
        info: Info {
            id: "example 2",
            label: "MF-JSON Example",
        },
        data: Data {
            fields: &["id".into()],
            rows: &rows,
        },
    };

    let output = File::options()
        .create(true)
        .write(true)
        .open("/tmp/out-kepler.txt")?;

    let serialized = serde_json::to_string(&ds).unwrap();
    writeln!(&output, "{}", &serialized)?;

    Ok(())
}
