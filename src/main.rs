use aws_lambda_events::event::sqs::SqsEvent;
use chrono:: Local ;
use dotenv::dotenv;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::types::time::Date;
use sqlx::{Pool, Postgres, Row};
use std::env;

enum SparkPlans {
    Hobby,      // 100
    Priority,   // 5000
    Enterprize, // Custom
}

impl SparkPlans {
    fn plan_from_str(plan_type: &str) -> Self {
        match plan_type {
            "Hobby" => SparkPlans::Hobby,
            "Priority" => SparkPlans::Priority,
            "Enterprize" => SparkPlans::Enterprize,
            _ => {
                panic!("Unknown plan type: {}", plan_type)
            }
        }
    }
}

async fn handle_plan_expiration(apikey: &str, db_client: &Pool<Postgres>) {

    // send some notification to user that the plan has expired ...

    let _ = sqlx::query("UPDATE UserDetails SET plantype = 'Hobby' WHERE apikey = $1")
        .bind(apikey)
        .execute(db_client)
        .await
    ;

    let _ = sqlx::query("UPDATE UserRequests SET plantype = 'Hobby', hits = 1 WHERE apikey = $1")
        .bind(apikey)
        .execute(db_client)
        .await
    ;
}

async fn rate_limit(res: PgRow, db_client: &Pool<Postgres>, apikey: String) -> bool {

    let current_date: String = Local::now().date_naive().to_string();

    let (plan_type, hits, plan_expiry_date) = (
        res.try_get::<String, _>("plantype").unwrap(),
        res.try_get::<i64, _>("hits").unwrap(),
        res.try_get::<Date, _>("expiryon").unwrap(),
    );

    match SparkPlans::plan_from_str(&plan_type) {

        SparkPlans::Hobby => {
            if hits > 100 {

                let _ = sqlx::query(
                    "UPDATE UserKeyStatus SET status = 'Daily limit reached' WHERE apikey = $1;"
                    )
                    .bind(&apikey)
                    .execute(db_client)
                    .await
                ;

                println!("A user with hobby plan reached its daily limit");
            } 
            else {

                let _ = sqlx::query(
                    "UPDATE UserRequests SET hits = hits + 1 WHERE apikey = $1;"
                    )
                    .bind(&apikey)
                    .execute(db_client)
                    .await
                ;

                println!("A user with hobby plan incremented its hits !!");
            }
        }

        SparkPlans::Priority => {

            if plan_expiry_date.to_string() == current_date {
                handle_plan_expiration(&apikey, db_client).await;
                println!("User's priority plan with key: {} has expired !", apikey);
                return true;   
            }

            if hits > 5000 {
                let _ = sqlx::query(
                    "UPDATE UserKeyStatus SET status = 'Daily limit reached' WHERE apikey = $1;"
                    )
                    .bind(&apikey)
                    .execute(db_client)
                    .await
                ;
            } 

            else {
                let _ = sqlx::query(
                    "UPDATE UserRequests SET hits = hits + 1 WHERE apikey = $1;"
                    )
                    .bind(&apikey)
                    .execute(db_client)
                    .await
                ;
            }

            println!("A user with priority plan reached its daily limit");
        }

        SparkPlans::Enterprize => {
            if plan_expiry_date.to_string() == current_date {
                println!("PLAN EXPIRED RE-SUBSCRIBE TO YOUR PRIORITY PLAN TO CONTINUE !!");
            }
            // we need to handle enterprize customers as well here ... 
            println!("ENTERPIZE USER !!");
        }
    }

    return true;
}

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<i32, Error> {

    dotenv().ok();
    let url = env::var("DB_URL").expect("DB Connection URL not found !!");

    let db_client = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?
    ;

    for mes in event.payload.records.iter() {
        println!("AUTH-SQS EVENT NO. {:?}: {:?}", mes.message_id, mes.body);

        let res = sqlx::query(r#"SELECT * FROM UserRequests WHERE apiKey = $1;"#)
            .bind(mes.body.clone().unwrap())
            .fetch_one(&db_client)
            .await?
        ;

        rate_limit(res, &db_client, mes.body.clone().unwrap().to_string()).await;
    }
    Ok(200)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    println!("MAIN FUNCTION: GOT {:?}", run(service_fn(function_handler)).await?);
    Ok(())
}