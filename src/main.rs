mod model;
mod operation;

use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Result};
use dotenv::dotenv;
use std::env;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const OPERATION_TARGET_HEADER: &str = "X-Amz-Target";

const OPERATION_NAME_START_QUERY_EXECUTION: &str = "AmazonAthena.StartQueryExecution";
const OPERATION_NAME_GET_QUERY_EXECUTION: &str = "AmazonAthena.GetQueryExecution";
const OPERATION_NAME_GET_QUERY_RESULTS: &str = "AmazonAthena.GetQueryResults";

async fn root(
    req: HttpRequest,
    input: web::Json<crate::model::Param>,
    data: web::Data<crate::model::AppData>,
) -> Result<HttpResponse> {
    let target = req.headers().get(OPERATION_TARGET_HEADER).ok_or_else(|| {
        HttpResponse::BadRequest().body(format!("'{:}' not found", OPERATION_TARGET_HEADER))
    })?;

    if target == OPERATION_NAME_START_QUERY_EXECUTION {
        operation::start_query_execution(input.deref(), data.get_ref())
    } else if target == OPERATION_NAME_GET_QUERY_EXECUTION {
        operation::get_query_execution(input.deref(), data.get_ref())
    } else if target == OPERATION_NAME_GET_QUERY_RESULTS {
        operation::get_query_results(input.deref(), data.get_ref())
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
            .app_data(web::Data::new(crate::model::AppData {
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
