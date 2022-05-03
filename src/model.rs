use aws_sdk_athena::model::QueryExecutionState;
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
    #[serde(rename = "ResultSet")]
    pub result_set: ResultSet,
    #[serde(rename = "NextToken")]
    pub next_token: Option<String>,
}

#[derive(serde::Serialize)]
pub struct ResultSet {
    #[serde(rename = "Rows")]
    pub rows: Vec<Row>,
    #[serde(rename = "ResultSetMetadata")]
    pub result_set_metadata: ResultSetMetadata,
}

#[derive(serde::Serialize)]
pub struct ResultSetMetadata {
    #[serde(rename = "ColumnInfo")]
    pub column_info: Vec<ColumnInfo>,
}

#[derive(serde::Serialize)]
pub struct ColumnInfo {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Label")]
    pub label: String,
}

#[derive(serde::Serialize)]
pub struct Row {
    #[serde(rename = "Data")]
    pub data: Vec<Datum>,
}

#[derive(serde::Serialize)]
pub struct Datum {
    #[serde(rename = "VarCharValue")]
    pub var_char_value: String,
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
