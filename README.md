# Minerva

Minerva is a [Amazon Athena](https://aws.amazon.com/athena/) mock server.

## Usage

* create fixture csv file (Minerva treats csv file name as table name)

```sh
> echo 'id,name
1,zaneli
2,athena
3,minerva' > users.csv
```

* start minerva server

```sh
> cargo run
```

* Run Athena CLI or other SDK

```sh
> aws --endpoint-url="http://localhost:5050" athena start-query-execution --query-string="SELECT * FROM users;"
{
    "QueryExecutionId": "3b773ac8-6a53-49e4-83d9-2cbf2ce51fa5"
}

> aws --endpoint-url="http://localhost:5050" athena get-query-execution --query-execution-id="3b773ac8-6a53-49e4-83d9-2cbf2ce51fa5"
{
    "QueryExecution": {
        "QueryExecutionId": "3b773ac8-6a53-49e4-83d9-2cbf2ce51fa5",
        "Status": {
            "State": "SUCCEEDED"
        }
    }
}

> aws --endpoint-url="http://localhost:5050" athena get-query-results --query-execution-id="3b773ac8-6a53-49e4-83d9-2cbf2ce51fa5"
{
    "ResultSet": {
        "Rows": [
            {
                "Data": [
                    {
                        "VarCharValue": "id"
                    },
                    {
                        "VarCharValue": "name"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "1"
                    },
                    {
                        "VarCharValue": "zaneli"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "2"
                    },
                    {
                        "VarCharValue": "athena"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "3"
                    },
                    {
                        "VarCharValue": "minerva"
                    }
                ]
            }
        ],
        "ResultSetMetadata": {
            "ColumnInfo": [
                {
                    "TableName": "users",
                    "Name": "id",
                    "Label": "id"
                },
                {
                    "TableName": "users",
                    "Name": "name",
                    "Label": "name"
                }
            ]
        }
    },
    "UpdateCount": 0
}

> aws --endpoint-url="http://localhost:5050" athena get-query-results --query-execution-id="3b773ac8-6a53-49e4-83d9-2cbf2ce51fa5" --max-results=2
{
    "UpdateCount": 0,
    "ResultSet": {
        "Rows": [
            {
                "Data": [
                    {
                        "VarCharValue": "id"
                    },
                    {
                        "VarCharValue": "name"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "1"
                    },
                    {
                        "VarCharValue": "zaneli"
                    }
                ]
            }
        ],
        "ResultSetMetadata": {
            "ColumnInfo": [
                {
                    "TableName": "users",
                    "Name": "id",
                    "Label": "id"
                },
                {
                    "TableName": "users",
                    "Name": "name",
                    "Label": "name"
                }
            ]
        }
    },
    "NextToken": "2"
}
```

## Support API

### [StartQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html)

- Request Parameters
  - [x] [QueryString](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html#athena-StartQueryExecution-request-QueryString)
- Response Syntax
  - [x] [QueryExecutionId](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html#athena-StartQueryExecution-response-QueryExecutionId)

### [GetQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryExecution.html)

- Request Parameters
  - [x] [QueryExecutionId](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryExecution.html#athena-GetQueryExecution-request-QueryExecutionId)
- Response Syntax
  - [x] [QueryExecution.QueryExecutionId](https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecution.html#athena-Type-QueryExecution-QueryExecutionId)
  - [x] [QueryExecution.Status.State](https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatus.html#athena-Type-QueryExecutionStatus-State)

### [GetQueryResults](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html)

- Request Parameters
  - [x] [MaxResults](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html#athena-GetQueryResults-request-MaxResults)
  - [x] [NextToken](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html#athena-GetQueryResults-request-NextToken)
  - [x] [QueryExecutionId](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html#athena-GetQueryResults-request-QueryExecutionId)
- Response Syntax
  - [x] [NextToken](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html#athena-GetQueryResults-response-NextToken)
  - [x] [ResultSet.ResultSetMetadata.ColumnInfo.TableName](https://docs.aws.amazon.com/athena/latest/APIReference/API_ColumnInfo.html#athena-Type-ColumnInfo-TableNames)
  - [x] [ResultSet.ResultSetMetadata.ColumnInfo.Name](https://docs.aws.amazon.com/athena/latest/APIReference/API_ColumnInfo.html#athena-Type-ColumnInfo-Name)
  - [x] [ResultSet.ResultSetMetadata.ColumnInfo.Label](https://docs.aws.amazon.com/athena/latest/APIReference/API_ColumnInfo.html#athena-Type-ColumnInfo-Label)
  - [x] [ResultSet.Rows.Data.VarCharValue](https://docs.aws.amazon.com/athena/latest/APIReference/API_Datum.html#athena-Type-Datum-VarCharValue)
