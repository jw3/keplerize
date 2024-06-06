use serde::{Deserialize, Serialize, Serializer};
use serde::ser::SerializeMap;

#[typetag::serde(tag = "type")]
pub trait Row {}

#[derive(Serialize, Debug)]
pub struct Dataset<'a, R: Row> {
    pub info: Info<'a>,
    pub data: Data<'a, R>,
}
#[derive(Serialize, Debug)]
pub struct Info<'a> {
    pub id: &'a str,
    pub label: &'a str,
}

#[derive(Debug)]
pub struct Data<'a, R: Row> {
    pub fields: &'a [Field<'a>],
    pub rows: &'a [R],
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub struct LineString {
    pub coordinates: Vec<Vec<f64>>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub struct Feature<G> {
    pub geometry: G,
}

#[derive(Clone, Serialize, Debug)]
pub struct Field<'a> {
    pub name: &'a str,
}

impl<'a> From<&'a str> for Field<'a> {
    fn from(name: &'a str) -> Self {
        Field { name }
    }
}

impl<'a, R: Row + Serialize> Serialize for Data<'a, R> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry(
            "fields",
            &[&[Field { name: "_geojson" }], self.fields].concat(),
        )?;
        map.serialize_entry("rows", self.rows)?;
        map.end()
    }
}
