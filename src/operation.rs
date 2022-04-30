use actix_rt::spawn;
use actix_rt::time;
use actix_web::{HttpResponse, Result};
use aws_sdk_athena::model::{Datum, QueryExecutionState, ResultSet, Row};
use sqlparser::ast::{ObjectName, SetExpr, Statement, TableFactor};
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
    let ast = Parser::parse_sql(&dialect, &query_string).map_err(|_| {
        HttpResponse::BadRequest().body(format!("invalid query: {:}", query_string))
    })?;
    if ast.len() != 1 {
        return Ok(HttpResponse::BadRequest().body(format!(
            "unsupported query: {:}, ast.len() = {:}",
            query_string,
            ast.len()
        )));
    }
    let table_name = match &ast[0] {
        Statement::Query(query) => match &query.body {
            SetExpr::Select(select) => {
                if select.from.len() != 1 {
                    return Ok(HttpResponse::BadRequest().body(format!(
                        "unsupported query: {:}, select.from.len() = {:}",
                        query_string,
                        select.from.len()
                    )));
                }
                match &select.from[0].relation {
                    TableFactor::Table {
                        name: ObjectName(name),
                        alias: _,
                        args: _,
                        with_hints: _,
                    } => {
                        if name.len() == 1 {
                            // Note: only `tablename`
                            &name[0].value
                        } else if name.len() == 2 {
                            // Note: `databasename.tablename`
                            &name[1].value
                        } else {
                            return Ok(HttpResponse::BadRequest().body(format!(
                                "unsupported query: {:}, name.len() = {:}",
                                query_string,
                                name.len()
                            )));
                        }
                    }
                    relation => {
                        return Ok(HttpResponse::BadRequest().body(format!(
                            "unsupported query: {:}, relation = {:?}",
                            query_string, relation
                        )))
                    }
                }
            }
            stmt => {
                return Ok(HttpResponse::BadRequest().body(format!(
                    "unsupported query: {:}, statement = {:?}",
                    query_string, stmt
                )))
            }
        },
        _ => {
            return Ok(
                HttpResponse::BadRequest().body(format!("unsupported query: {:}", query_string))
            )
        }
    };

    let query_execution_id = Uuid::new_v4().to_string();
    process_query(
        query_execution_id.clone(),
        table_name.clone(),
        data.process_interval,
        data.processes_r.clone(),
        data.processes_w.clone(),
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
    let query_process = data
        .processes_r
        .get_one::<String>(&query_execution_id)
        .ok_or_else(|| {
            HttpResponse::BadRequest().body("query_execution_id not found".to_string())
        })?;
    let state = QueryExecutionState::from(query_process.state.as_ref());

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
    let query_process = data
        .processes_r
        .get_one::<String>(&query_execution_id)
        .ok_or_else(|| {
            HttpResponse::BadRequest().body("query_execution_id not found".to_string())
        })?;
    let state = QueryExecutionState::from(query_process.state.as_ref());
    if state != QueryExecutionState::Succeeded {
        return Ok(HttpResponse::BadRequest().body(format!("state not succeeded yet: {:?}", state)));
    }

    let mut rows = Vec::new();
    rows.push(
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
    );
    let next_token = if input.next_token.is_none() {
        rows.push(
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
        );
        rows.push(
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
        );
        Some("dummy_token".to_string())
    } else {
        rows.push(
            Row::builder()
                .set_data(Some(vec![
                    Datum::builder()
                        .set_var_char_value(Some("2014-07-05".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("SFO4".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("Firefox".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("/test-image-3.jpeg".to_string()))
                        .build(),
                    Datum::builder()
                        .set_var_char_value(Some("200".to_string()))
                        .build(),
                ]))
                .build(),
        );
        None
    };
    Ok(
        HttpResponse::Ok().json(crate::model::GetQueryResultsResponse {
            result_set: ResultSet::builder().set_rows(Some(rows)).build(),
            next_token: next_token,
            update_count: 0,
        }),
    )
}

fn process_query(
    query_execution_id: String,
    table_name: String,
    process_interval: Duration,
    processes_r: evmap::ReadHandle<String, crate::model::QueryProcess>,
    processes_w: Arc<Mutex<evmap::WriteHandle<String, crate::model::QueryProcess>>>,
) {
    spawn(async move {
        let mut interval = time::interval(process_interval);
        loop {
            let query_process = processes_r.get_one::<String>(&query_execution_id);
            let mut processes_w = processes_w.lock().unwrap();
            match query_process {
                Some(query_process) => {
                    if query_process.state.to_string() == QueryExecutionState::Queued.as_str() {
                        processes_w.empty(query_execution_id.clone());
                        processes_w.insert(
                            query_execution_id.clone(),
                            crate::model::QueryProcess {
                                table_name: table_name.clone(),
                                state: QueryExecutionState::Running.as_str().to_string(),
                            },
                        );
                    } else if query_process.state.to_string()
                        == QueryExecutionState::Running.as_str()
                    {
                        processes_w.empty(query_execution_id.clone());
                        processes_w.insert(
                            query_execution_id.clone(),
                            crate::model::QueryProcess {
                                table_name: table_name.clone(),
                                state: QueryExecutionState::Succeeded.as_str().to_string(),
                            },
                        );
                    } else if query_process.state.to_string()
                        == QueryExecutionState::Succeeded.as_str()
                    {
                        return;
                    }
                }
                None => {
                    processes_w.insert(
                        query_execution_id.clone(),
                        crate::model::QueryProcess {
                            table_name: table_name.clone(),
                            state: QueryExecutionState::Queued.as_str().to_string(),
                        },
                    );
                }
            }
            processes_w.refresh();

            interval.tick().await;
        }
    })
}
