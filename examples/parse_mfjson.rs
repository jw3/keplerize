use chrono::DateTime;
use serde::{Deserialize, Deserializer, Serialize};
use serde::de::Error;

use keplerviz::{Data, Dataset, Feature, Info, LineString, Row};

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

fn main() {
    let input = r#"{"id":367716330,"json":{
    "type":"MovingPoint",
    "coordinates":[[-89.90329,29.89745],[-89.9033,29.89743],[-89.90324,29.89739],[-89.90326,29.89741],[-89.90325,29.89742],[-89.90325,29.89742]],
    "datetimes":["2019-12-31T19:00:08-05","2019-12-31T19:01:12-05","2019-12-31T19:02:22-05","2019-12-31T19:03:32-05","2019-12-31T19:04:42-05","2019-12-31T19:07:10-05"],
    "lower_inc":true,"upper_inc":true,"interpolation":"Linear"}}"#;

    // let rec = DateTime::parse_from_str("2019-12-31T19:01:12-05", "%Y-%m-%dT%T%#z");
    let rec: Rec = serde_json::from_str(input).unwrap();
    // dbg!(&rec);

    let source_data = vec![rec];
    let rows: Vec<MyRow> = source_data.into_iter().map(MyRow::from).collect();
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

    let serialized = serde_json::to_string_pretty(&ds).unwrap();
    println!("{}", serialized);
}
