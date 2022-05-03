use aws_sdk_athena::model::{QueryExecutionState, ResultSet};
use serde::ser::SerializeStruct;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(serde::Serialize)]
pub struct StartQueryExecutionResponse {
    #[serde(rename = "QueryExecutionId")]
    pub query_execution_id: String,
}

#[derive(serde::Serialize)]
pub struct GetQueryExecutionResponse {
    #[serde(rename = "QueryExecution")]
    pub query_execution: QueryExecutionResponse,
}

#[derive(serde::Serialize)]
pub struct GetQueryResultsResponse {
    #[serde(rename = "UpdateCount")]
    pub update_count: u32,
    #[serde(rename = "ResultSet", serialize_with = "serialize_result_set")]
    pub result_set: ResultSet,
    #[serde(rename = "NextToken")]
    pub next_token: Option<String>,
}

fn serialize_result_set<S: serde::Serializer>(
    result_set: &ResultSet,
    s: S,
) -> Result<S::Ok, S::Error> {
    let mut ss = s.serialize_struct("ResultSet", 1)?;
    let rows = result_set.rows();
    let mut row = Vec::new();
    if let Some(rows) = rows {
        for r in rows {
            row.push(Row::from(r.clone()));
        }
    };
    ss.serialize_field("Rows", &row)?;
    ss.end()
}

#[derive(serde::Serialize)]
struct Row {
    #[serde(rename = "Data")]
    data: Vec<Datum>,
}

impl Row {
    fn from(row: aws_sdk_athena::model::Row) -> Self {
        let mut data = Vec::new();
        if let Some(d) = row.data {
            for datum in d {
                if let Some(v) = datum.var_char_value {
                    data.push(Datum {
                        var_char_value: v.clone(),
                    });
                }
            }
        }
        Row { data: data }
    }
}

#[derive(serde::Serialize)]
struct Datum {
    #[serde(rename = "VarCharValue")]
    var_char_value: String,
}

#[derive(serde::Serialize)]
pub struct QueryExecutionResponse {
    #[serde(rename = "QueryExecutionId")]
    pub query_execution_id: String,
    #[serde(rename = "Status")]
    pub status: StatusResponse,
}

#[derive(serde::Serialize)]
pub struct StatusResponse {
    #[serde(rename = "State", serialize_with = "serialize_state")]
    pub state: QueryExecutionState,
}

fn serialize_state<S: serde::Serializer>(
    state: &QueryExecutionState,
    s: S,
) -> Result<S::Ok, S::Error> {
    s.serialize_str(state.as_ref())
}

#[derive(serde::Deserialize)]
pub struct Param {
    #[serde(rename = "QueryExecutionId")]
    pub query_execution_id: Option<String>,
    #[serde(rename = "QueryString")]
    pub query_string: Option<String>,
    #[serde(rename = "NextToken")]
    pub next_token: Option<String>,
    #[serde(rename = "MaxResults")]
    pub max_results: Option<u64>,
}

pub struct AppData {
    pub process_interval: Duration,
    pub csv_fixture_dir: String,
    pub processes_r: evmap::ReadHandle<String, QueryProcess>,
    pub processes_w: Arc<Mutex<evmap::WriteHandle<String, QueryProcess>>>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, evmap_derive::ShallowCopy)]
pub struct QueryProcess {
    pub table_name: String,
    pub state: String,
}
