use aws_lambda_events::event::sqs::SqsEvent;
use chrono::{ Local, NaiveDate, ParseError };
use dotenv::dotenv;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::types::time::Date;
use sqlx::{Pool, Postgres, Row};
use std::{ cmp::Ordering, env};

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

fn compare_dates(date1: &str, date2: &str) -> Result<Ordering, ParseError> {
    // Parse the strings into NaiveDate
    let parsed_date1 = NaiveDate::parse_from_str(date1, "%Y-%m-%d");
    let parsed_date2 = NaiveDate::parse_from_str(date2, "%Y-%m-%d");
    // Compare the parsed dates
    Ok(parsed_date1?.cmp(&parsed_date2?))
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

    let (plan_type, ref_date, hits, plan_expiry_date) = (
        res.try_get::<String, _>("plantype").unwrap(),
        res.try_get::<Date, _>("refdate").unwrap().to_string(),
        res.try_get::<i64, _>("hits").unwrap(),
        res.try_get::<Date, _>("expiryon").unwrap(),
    );

    let comapred_dates = compare_dates(&current_date, &ref_date).unwrap();

    // ref_date is outdated then ...
    if comapred_dates == Ordering::Greater {

        // hits reseted and ref_date updated to current_date
        let _ = sqlx::query(
            "UPDATE UserRequests SET refdate = CURRENT_DATE, hits = 1 WHERE apikey = $1;"
            )
            .bind(&apikey)
            .execute(db_client)
            .await
        ;

        println!("Updated the ref_date to current_date & hits to 1.");
    }
    
    if comapred_dates == Ordering::Equal {  //  same din hai, sirf hits ko increment karna hai

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

                if plan_expiry_date.to_string() == Local::now().date_naive().to_string() {
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
                if plan_expiry_date.to_string() == Local::now().date_naive().to_string() {
                    println!("PLAN EXPIRED RE-SUBSCRIBE TO YOUR PRIORITY PLAN TO CONTINUE !!");
                }
                // we need to handle enterprize customers as well here ... 
                println!("ENTERPIZE USER !!");
            }
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