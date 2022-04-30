use aws_sdk_athena::model::{QueryExecutionState, ResultSet};
use serde::ser::SerializeStruct;

#[derive(serde::Serialize)]
#[allow(non_snake_case)]
pub struct StartQueryExecutionResponse {
    pub QueryExecutionId: String,
}

#[derive(serde::Serialize)]
#[allow(non_snake_case)]
pub struct GetQueryExecutionResponse {
    pub QueryExecution: QueryExecutionResponse,
}

#[derive(serde::Serialize)]
#[allow(non_snake_case)]
pub struct GetQueryResultsResponse {
    pub UpdateCount: u32,
    #[serde(serialize_with = "serialize_result_set")]
    pub ResultSet: ResultSet,
    pub NextToken: Option<String>,
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
#[allow(non_snake_case)]
struct Row {
    Data: Vec<Datum>,
}

impl Row {
    fn from(row: aws_sdk_athena::model::Row) -> Self {
        let mut data = Vec::new();
        if let Some(d) = row.data {
            for datum in d {
                if let Some(v) = datum.var_char_value {
                    data.push(Datum {
                        VarCharValue: v.clone(),
                    });
                }
            }
        }
        Row { Data: data }
    }
}

#[derive(serde::Serialize)]
#[allow(non_snake_case)]
struct Datum {
    VarCharValue: String,
}

#[derive(serde::Serialize)]
#[allow(non_snake_case)]
pub struct QueryExecutionResponse {
    pub QueryExecutionId: String,
    pub Status: StatusResponse,
}

#[derive(serde::Serialize)]
#[allow(non_snake_case)]
pub struct StatusResponse {
    #[serde(serialize_with = "serialize_state")]
    pub State: QueryExecutionState,
}

fn serialize_state<S: serde::Serializer>(
    state: &QueryExecutionState,
    s: S,
) -> Result<S::Ok, S::Error> {
    s.serialize_str(state.as_ref())
}

#[derive(Debug, serde::Deserialize)]
#[allow(non_snake_case)]
pub struct Param {
    pub QueryExecutionId: Option<String>,
    pub QueryString: Option<String>,
}
