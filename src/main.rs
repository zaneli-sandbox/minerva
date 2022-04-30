mod model;

use actix_rt::spawn;
use actix_rt::time;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Result};
use aws_sdk_athena::model::{Datum, QueryExecutionState, ResultSet, Row};
use dotenv::dotenv;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

const OPERATION_TARGET_HEADER: &str = "X-Amz-Target";

const OPERATION_NAME_START_QUERY_EXECUTION: &str = "AmazonAthena.StartQueryExecution";
const OPERATION_NAME_GET_QUERY_EXECUTION: &str = "AmazonAthena.GetQueryExecution";
const OPERATION_NAME_GET_QUERY_RESULTS: &str = "AmazonAthena.GetQueryResults";

async fn root(
    req: HttpRequest,
    input: Option<web::Json<crate::model::Param>>,
    data: web::Data<AppData>,
) -> Result<HttpResponse> {
    let target = req.headers().get(OPERATION_TARGET_HEADER).ok_or_else(|| {
        HttpResponse::BadRequest().body(format!("'{:}' not found", OPERATION_TARGET_HEADER))
    })?;

    if target == OPERATION_NAME_START_QUERY_EXECUTION {
        let input =
            input.ok_or_else(|| HttpResponse::BadRequest().body("unexpected input".to_string()))?;
        let query_string = input
            .QueryString
            .clone()
            .ok_or_else(|| HttpResponse::BadRequest().body("unexpected input".to_string()))?;
        let dialect = GenericDialect {};
        let _ = Parser::parse_sql(&dialect, &query_string).map_err(|_| {
            HttpResponse::BadRequest().body(format!("invalid query: {:}", query_string))
        })?;

        let query_execution_id = Uuid::new_v4().to_string();
        process_query(
            query_execution_id.clone(),
            data.process_interval,
            data.queries_r.clone(),
            data.queries_w.clone(),
        );

        Ok(
            HttpResponse::Ok().json(crate::model::StartQueryExecutionResponse {
                QueryExecutionId: query_execution_id.clone(),
            }),
        )
    } else if target == OPERATION_NAME_GET_QUERY_EXECUTION {
        let input =
            input.ok_or_else(|| HttpResponse::BadRequest().body("unexpected input".to_string()))?;
        let query_execution_id = input
            .QueryExecutionId
            .clone()
            .ok_or_else(|| HttpResponse::BadRequest().body("unexpected input".to_string()))?;
        let state = data
            .queries_r
            .get_one::<String>(&query_execution_id)
            .ok_or_else(|| {
                HttpResponse::BadRequest().body("query_execution_id not found".to_string())
            })?;
        let state = QueryExecutionState::from(state.to_string().as_ref());

        Ok(
            HttpResponse::Ok().json(crate::model::GetQueryExecutionResponse {
                QueryExecution: crate::model::QueryExecutionResponse {
                    QueryExecutionId: query_execution_id.clone(),
                    Status: crate::model::StatusResponse { State: state },
                },
            }),
        )
    } else if target == OPERATION_NAME_GET_QUERY_RESULTS {
        let input =
            input.ok_or_else(|| HttpResponse::BadRequest().body("unexpected input".to_string()))?;
        let query_execution_id = input
            .QueryExecutionId
            .clone()
            .ok_or_else(|| HttpResponse::BadRequest().body("unexpected input".to_string()))?;
        let state = data
            .queries_r
            .get_one::<String>(&query_execution_id)
            .ok_or_else(|| {
                HttpResponse::BadRequest().body("query_execution_id not found".to_string())
            })?;
        let state = QueryExecutionState::from(state.to_string().as_ref());
        if state != QueryExecutionState::Succeeded {
            return Ok(
                HttpResponse::BadRequest().body(format!("state not succeeded yet: {:?}", state))
            );
        }

        let rows = vec![
            Row::builder()
                .set_data(Some(vec![
                    Datum::builder()
                        .set_var_char_value(Some("date".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("location".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("browser".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("uri".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("status".to_string()))
                        .build(),
                ]))
                .build(),
            Row::builder()
                .set_data(Some(vec![
                    Datum::builder()
                        .set_var_char_value(Some("2014-07-05".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("SFO4".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("Safari".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("/test-image-2.jpeg".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("200".to_string()))
                        .build(),
                ]))
                .build(),
            Row::builder()
                .set_data(Some(vec![
                    Datum::builder()
                        .set_var_char_value(Some("2014-07-05".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("SFO4".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("Opera".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("/test-image-2.jpeg".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("200".to_string()))
                        .build(),
                ]))
                .build(),
        ];
        Ok(
            HttpResponse::Ok().json(crate::model::GetQueryResultsResponse {
                ResultSet: ResultSet::builder().set_rows(Some(rows)).build(),
                NextToken: None,
                UpdateCount: 0,
            }),
        )
    } else {
        Ok(HttpResponse::BadRequest().body(format!("unexpected target: {:?}", target)))
    }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    let port = env::var("PORT").unwrap_or("5050".to_string());
    let process_interval = env::var("PROCESS_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5);

    let (queries_r, queries_w) = evmap::new();
    let queries_w = Arc::new(Mutex::new(queries_w));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppData {
                process_interval: Duration::from_secs(process_interval),
                queries_r: queries_r.clone(),
                queries_w: queries_w.clone(),
            }))
            .app_data(web::JsonConfig::default().content_type(|mime| {
                mime.type_() == mime::APPLICATION
                    && mime.subtype().to_string().starts_with("x-amz-json-")
            }))
            .route("/", web::post().to(root))
    })
    .bind(format!("127.0.0.1:{:}", port))?
    .run()
    .await
}

fn process_query(
    query_execution_id: String,
    process_interval: Duration,
    queries_r: evmap::ReadHandle<String, String>,
    queries_w: Arc<Mutex<evmap::WriteHandle<String, String>>>,
) {
    spawn(async move {
        let mut interval = time::interval(process_interval);
        loop {
            let query = queries_r.get_one::<String>(&query_execution_id);
            let mut queries_w = queries_w.lock().unwrap();
            match query {
                Some(state) => {
                    if state.to_string() == QueryExecutionState::Queued.as_str() {
                        queries_w.empty(query_execution_id.clone());
                        queries_w.insert(
                            query_execution_id.clone(),
                            QueryExecutionState::Running.as_str().to_string(),
                        );
                    } else if state.to_string() == QueryExecutionState::Running.as_str() {
                        queries_w.empty(query_execution_id.clone());
                        queries_w.insert(
                            query_execution_id.clone(),
                            QueryExecutionState::Succeeded.as_str().to_string(),
                        );
                    } else if state.to_string() == QueryExecutionState::Succeeded.as_str() {
                        return;
                    }
                }
                None => {
                    queries_w.insert(
                        query_execution_id.clone(),
                        QueryExecutionState::Queued.as_str().to_string(),
                    );
                }
            }
            queries_w.refresh();

            interval.tick().await;
        }
    })
}

pub struct AppData {
    process_interval: Duration,
    queries_r: evmap::ReadHandle<String, String>,
    queries_w: Arc<Mutex<evmap::WriteHandle<String, String>>>,
}
