use actix_rt::spawn;
use actix_rt::time;
use actix_web::{HttpResponse, Result};
use aws_sdk_athena::model::{Datum, QueryExecutionState, ResultSet, Row};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

pub fn start_query_execution(
    input: &crate::model::Param,
    data: &crate::model::AppData,
) -> Result<HttpResponse> {
    let query_string = input
        .query_string
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
            query_execution_id: query_execution_id.clone(),
        }),
    )
}

pub fn get_query_execution(
    input: &crate::model::Param,
    data: &crate::model::AppData,
) -> Result<HttpResponse> {
    let query_execution_id = input
        .query_execution_id
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
            query_execution: crate::model::QueryExecutionResponse {
                query_execution_id: query_execution_id.clone(),
                status: crate::model::StatusResponse { state: state },
            },
        }),
    )
}

pub fn get_query_results(
    input: &crate::model::Param,
    data: &crate::model::AppData,
) -> Result<HttpResponse> {
    let query_execution_id = input
        .query_execution_id
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
        return Ok(HttpResponse::BadRequest().body(format!("state not succeeded yet: {:?}", state)));
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
            result_set: ResultSet::builder().set_rows(Some(rows)).build(),
            next_token: None,
            update_count: 0,
        }),
    )
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
