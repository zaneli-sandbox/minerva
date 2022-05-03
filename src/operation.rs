use actix_rt::spawn;
use actix_rt::time;
use actix_web::{HttpResponse, Result};
use aws_sdk_athena::model::QueryExecutionState;
use sqlparser::ast::{ObjectName, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fs::File;
use std::io::BufReader;
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
        HttpResponse::Ok().json(crate::model::StartQueryExecutionResponse::new(
            query_execution_id,
        )),
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
        HttpResponse::Ok().json(crate::model::GetQueryExecutionResponse::new(
            query_execution_id,
            state,
        )),
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

    let table_name = &query_process.table_name;
    let f = File::open(format!("{:}/{:}.csv", data.csv_fixture_dir, table_name))?;
    let b = BufReader::new(f);
    let mut csv_reader = csv::ReaderBuilder::new().has_headers(true).from_reader(b);

    let mut column_names = Vec::new();
    for header in csv_reader
        .headers()
        .map_err(|_| HttpResponse::BadRequest().body("failed to read csv headers".to_string()))?
    {
        column_names.push(header.to_string());
    }

    let mut rows = Vec::new();
    let offset = input
        .next_token
        .as_ref()
        .unwrap_or(&"0".to_string())
        .parse::<u64>()
        .map_err(|_| HttpResponse::BadRequest().body("invalid next_token".to_string()))?;
    let limit = input.max_results.unwrap_or(100) + offset;

    let mut count = 0;
    if input.next_token.is_none() {
        let _ = count + 1;
        rows.push(crate::model::Row::new(&column_names));
    }

    let mut next_token = None;
    for records in csv_reader.records() {
        count += 1;
        if count < offset {
            continue;
        }
        if count > limit {
            next_token = Some(count.to_string());
            break;
        }
        let rs = records.map_err(|_| {
            HttpResponse::BadRequest().body("failed to read csv fixture".to_string())
        })?;

        let mut records = Vec::new();
        for record in rs.iter() {
            records.push(crate::model::Datum {
                var_char_value: record.to_string(),
            });
        }
        rows.push(crate::model::Row { data: records });
    }

    Ok(
        HttpResponse::Ok().json(crate::model::GetQueryResultsResponse::new(
            table_name.to_string(),
            column_names,
            rows,
            next_token,
        )),
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
            let query_state = processes_r
                .get_one::<String>(&query_execution_id)
                .map(|v| QueryExecutionState::from(v.state.as_ref()));
            let mut processes_w = processes_w.lock().unwrap();
            match query_state {
                None => {
                    processes_w.insert(
                        query_execution_id.clone(),
                        crate::model::QueryProcess {
                            table_name: table_name.clone(),
                            state: QueryExecutionState::Queued.as_str().to_string(),
                        },
                    );
                }
                Some(QueryExecutionState::Queued) => {
                    processes_w.empty(query_execution_id.clone());
                    processes_w.insert(
                        query_execution_id.clone(),
                        crate::model::QueryProcess {
                            table_name: table_name.clone(),
                            state: QueryExecutionState::Running.as_str().to_string(),
                        },
                    );
                }
                Some(QueryExecutionState::Running) => {
                    processes_w.empty(query_execution_id.clone());
                    processes_w.insert(
                        query_execution_id.clone(),
                        crate::model::QueryProcess {
                            table_name: table_name.clone(),
                            state: QueryExecutionState::Succeeded.as_str().to_string(),
                        },
                    );
                }
                Some(QueryExecutionState::Succeeded) => return,
                Some(state) => panic!("unexpected state: {:?}", state),
            }
            processes_w.refresh();

            interval.tick().await;
        }
    })
}
