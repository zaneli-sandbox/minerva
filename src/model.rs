use aws_sdk_athena::model::QueryExecutionState;
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
pub struct QueryExecutionResponse {
    pub QueryExecutionId: String,
    pub Status: StatusResponse,
}

pub struct StatusResponse {
    pub state: QueryExecutionState,
}

impl serde::Serialize for StatusResponse {
    fn serialize<S>(&self, serializer: S) -> actix_web::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut s = serializer.serialize_struct("StatusResponse", 1)?;
        s.serialize_field("State", self.state.as_ref())?;
        s.end()
    }
}

#[derive(Debug, serde::Deserialize)]
#[allow(non_snake_case)]
pub struct Param {
    pub QueryExecutionId: String,
}
