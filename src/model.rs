use aws_sdk_athena::model::QueryExecutionState;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(serde::Serialize)]
pub struct StartQueryExecutionResponse {
    #[serde(rename = "QueryExecutionId")]
    query_execution_id: String,
}

impl StartQueryExecutionResponse {
    pub fn new(query_execution_id: String) -> Self {
        StartQueryExecutionResponse {
            query_execution_id: query_execution_id,
        }
    }
}

#[derive(serde::Serialize)]
pub struct GetQueryExecutionResponse {
    #[serde(rename = "QueryExecution")]
    query_execution: QueryExecutionResponse,
}

impl GetQueryExecutionResponse {
    pub fn new(query_execution_id: String, state: QueryExecutionState) -> Self {
        GetQueryExecutionResponse {
            query_execution: QueryExecutionResponse {
                query_execution_id: query_execution_id,
                status: StatusResponse { state: state },
            },
        }
    }
}

#[derive(serde::Serialize)]
pub struct GetQueryResultsResponse {
    #[serde(rename = "UpdateCount")]
    update_count: u32,
    #[serde(rename = "ResultSet")]
    result_set: ResultSet,
    #[serde(rename = "NextToken")]
    next_token: Option<String>,
}

impl GetQueryResultsResponse {
    pub fn new(
        table_name: String,
        column_names: Vec<String>,
        rows: Vec<Row>,
        next_token: Option<String>,
    ) -> Self {
        let mut column_info = Vec::new();
        for column_name in &column_names {
            column_info.push(ColumnInfo {
                table_name: table_name.clone(),
                name: column_name.clone(),
                label: column_name.clone(),
            });
        }
        GetQueryResultsResponse {
            result_set: ResultSet {
                rows: rows,
                result_set_metadata: ResultSetMetadata {
                    column_info: column_info,
                },
            },
            next_token: next_token,
            update_count: 0,
        }
    }
}

#[derive(serde::Serialize)]
struct ResultSet {
    #[serde(rename = "Rows")]
    rows: Vec<Row>,
    #[serde(rename = "ResultSetMetadata")]
    result_set_metadata: ResultSetMetadata,
}

#[derive(serde::Serialize)]
struct ResultSetMetadata {
    #[serde(rename = "ColumnInfo")]
    column_info: Vec<ColumnInfo>,
}

#[derive(serde::Serialize)]
struct ColumnInfo {
    #[serde(rename = "TableName")]
    table_name: String,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Label")]
    label: String,
}

#[derive(serde::Serialize)]
pub struct Row {
    #[serde(rename = "Data")]
    pub data: Vec<Datum>,
}

impl Row {
    pub fn new(values: &Vec<String>) -> Self {
        let mut data = Vec::new();
        for value in values {
            data.push(Datum {
                var_char_value: value.clone(),
            });
        }
        Row { data: data }
    }
}

#[derive(serde::Serialize)]
pub struct Datum {
    #[serde(rename = "VarCharValue")]
    pub var_char_value: String,
}

#[derive(serde::Serialize)]
struct QueryExecutionResponse {
    #[serde(rename = "QueryExecutionId")]
    query_execution_id: String,
    #[serde(rename = "Status")]
    status: StatusResponse,
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
