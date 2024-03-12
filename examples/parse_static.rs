use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use keplerviz::{Data, Dataset, Feature, Info, LineString, Row};

#[derive(Copy, Clone, Deserialize, Serialize, Debug)]
enum Vendor {
    A,
    B,
}

#[derive(Deserialize, Serialize, Debug)]
struct Source {
    id: u32,
    vendor: Vendor,
    coords: Vec<[f64; 2]>,
    time: Vec<f64>,
}

#[derive(Deserialize, Serialize, Debug)]
struct MyRow(Feature, u32, Vendor);

#[typetag::serde]
impl Row for MyRow {}

impl From<Source> for MyRow {
    fn from(src: Source) -> Self {
        assert_eq!(src.coords.len(), src.time.len());
        let coords = src
            .time
            .iter()
            .zip(src.coords)
            .into_iter()
            .map(|(&t, [x, y])| [x, y, 0.0, t]);
        let g = LineString {
            //geometry_type: "LineString",
            coordinates: coords.collect(),
        };
        MyRow(Feature { geometry: g }, src.id, src.vendor)
    }
}

fn main() {
    let source_data = [
        Source {
            id: 0,
            vendor: Vendor::A,
            coords: vec![
                [-74.20996, 40.81773],
                [-74.20997, 40.81765],
                [-74.20998, 40.81],
            ],
            time: vec![1564184363.0, 1564184396.0, 1564184409.0],
        },
        Source {
            id: 1,
            vendor: Vendor::B,
            coords: vec![
                [-74.20986, 40.81773],
                [-74.20987, 40.81765],
                [-74.20998, 40.81],
            ],
            time: vec![1564184363.0, 1564184396.0, 1564184409.0],
        },
    ];
    let rows: Vec<MyRow> = source_data.into_iter().map(MyRow::from).collect();

    let ds = Dataset::<MyRow> {
        info: Info {
            id: "example",
            label: "My Example Dataset",
        },
        data: Data {
            fields: &["id".into(), "vendor".into()],
            rows: &rows,
        },
    };

    let serialized = serde_json::to_string_pretty(&ds).unwrap();
    println!("{}", serialized);
}
