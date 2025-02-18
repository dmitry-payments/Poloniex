mod api;
mod websocket;
mod db;
mod data_structs;
use websocket::start_ws;
use websocket::start_ws_trades;
use sqlx::postgres::PgPoolOptions;
use dotenvy::dotenv;
use std::{env, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
     dotenv().ok();
    println!("Загружены переменные окружения.");

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL не задан");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Не удалось подключиться к БД");

    println!("Подключение к БД установлено.");

    sqlx::migrate!("./migrations").run(&pool).await?;

    println!("Миграции успешно выполнены.");

    println!("Таблица `candles` готова.");

    for pair in data_structs::PAIRS.iter() {
        for interval in data_structs::INTERVALS.iter() {
            println!("Запрашиваем свечи для {} - {}", pair, interval);
    
            match api::get_candles(pair, interval).await {
                Ok(candles) => {
                    println!("Получено {} свечей для {} - {}", candles.len(), pair, interval);
                    
                    if candles.is_empty() {
                        println!("Пустой ответ от API для {} - {}", pair, interval);
                    }
    
                    if let Err(e) = db::insert_candles(&pool, candles).await {
                        eprintln!("Ошибка записи в БД: {}", e);
                    } else {
                        println!("Данные записаны в БД.");
                    }
                }
                Err(e) => {
                    eprintln!("Ошибка запроса {} {}: {}", pair, interval, e);
                }
            }
        }
    }

    println!("Данные успешно загружены в БД.");

    let pool = Arc::new(pool);

    start_ws(Arc::clone(&pool)).await;
    start_ws_trades(Arc::clone(&pool)).await;

    Ok(())
}
