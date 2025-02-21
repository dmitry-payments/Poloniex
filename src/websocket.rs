use futures_util::stream::All;
use tokio::task;
use tokio_tungstenite::connect_async;
use futures_util::{join, SinkExt, StreamExt};
use tokio::time::{sleep, interval, Duration};
use tokio_tungstenite::tungstenite::protocol::Message;
use serde_json::{json, Value};
use url::Url;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::data_structs::{Kline, VBS, PAIRS};
use crate::data_structs::RecentTrade;
use crate::{db};
use sqlx::{PgPool, Row};
use chrono::{Utc, Timelike};

const WS_URL: &str = "wss://ws.poloniex.com/ws/public";

// метод не используется
pub async fn start_ws(pool: Arc<PgPool>) {
    loop {
        match connect_async(Url::parse(WS_URL).unwrap()).await {
            Ok((ws_stream, _)) => {
                println!("Подключено к WebSocket!");

                let (write, mut read) = ws_stream.split();
                let write = Arc::new(Mutex::new(write));

                let subscribe_request = json!({
                    "event": "subscribe",
                    "channel": ["candles_minute_1", "candles_minute_15", "candles_hour_1", "candles_day_1"],
                    "symbols": ["btc_usdt", "trx_usdt", "eth_usdt", "doge_usdt", "bch_usdt"]
                });

                {
                    let mut write_guard = write.lock().await;
                    if let Err(e) = write_guard.send(Message::Text(subscribe_request.to_string())).await {
                        eprintln!("Ошибка подписки: {}", e);
                        continue;
                    }
                }

                println!("Запрос на подписку отправлен!");

                // Фоновая задача для отправки PING
                let write_clone = Arc::clone(&write);
                tokio::spawn(async move {
                    let mut ping_interval = interval(Duration::from_secs(29));
                    loop {
                        ping_interval.tick().await;
                        let mut write_guard = write_clone.lock().await;
                        if let Err(e) = write_guard.send(Message::Text(json!({"event": "ping"}).to_string())).await {
                            eprintln!("Ошибка отправки PING: {}", e);
                            break;
                        } else {
                            println!("PING отправлен");
                        }
                    }
                });

                // Фоновая задача для обработки сообщений и записи в БД
                let pool_clone = Arc::clone(&pool);
                tokio::spawn(async move {
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                println!("Получено сообщение: {}", text);
                                if let Some(kline) = parse_candle_message(&text) {
                                    if let Err(e) = db::insert_candles(&pool_clone, vec![kline]).await {
                                        eprintln!("Ошибка записи в БД: {}", e);
                                    } else {
                                        println!("Данные записаны в БД.");
                                    }
                                }
                            }
                            Ok(Message::Binary(bin)) => println!("сообщение: {:?}", bin),
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Ошибка WebSocket: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => eprintln!("Ошибка подключения: {}", e),
        }

        println!("Переподключение через 5 секунд...");
        sleep(Duration::from_secs(5)).await;
    }
}

// метод не используется
fn parse_candle_message(text: &str) -> Option<Kline> {
    let parsed: Value = serde_json::from_str(text).ok()?;
    let data_array = parsed.get("data")?.as_array()?;
    let row = data_array.get(0)?;
    
    // Извлекаем поля из объекта
    let symbol = row.get("symbol")?.as_str()?.to_string();
    let open = row.get("open")?.as_str()?.parse::<f64>().ok()?;
    let high = row.get("high")?.as_str()?.parse::<f64>().ok()?;
    let low = row.get("low")?.as_str()?.parse::<f64>().ok()?;
    let close = row.get("close")?.as_str()?.parse::<f64>().ok()?;
    let quantity = row.get("quantity")?.as_str()?.parse::<f64>().ok()?;
    let amount = row.get("amount")?.as_str()?.parse::<f64>().ok()?;
    let start_time = row.get("startTime")?.as_i64()?;
    
    let volume_bs = VBS {
        buy_base: 0.0,
        sell_base: quantity,
        buy_quote: 0.0,
        sell_quote: amount,
    };
    
    // время интервала
    let channel = parsed.get("channel")?.as_str()?;
    let time_frame = channel
        .split('_')
        .skip(1)
        .collect::<Vec<&str>>()
        .join("_")
        .to_uppercase();
    
    Some(Kline {
        pair: symbol,
        time_frame,
        open,
        high,
        low,
        close,
        volume_bs,
        utc_begin: start_time,
    })
}

async fn agg_candles(pool: Arc<PgPool>, text: &str, duration: i64) {
    loop {
        sleep(Duration::from_secs((60 * duration) as u64)).await;
        let now = Utc::now();
        let start = now.date_naive().and_hms(now.hour(), now.minute(), 0).timestamp_millis();
        let end = start + 60000 * duration;
        println!("Агрегация свечей {}: {} - {}", text, start, end);
        for &pair in PAIRS.iter() {
            if let Err(e) = aggregate_trades_to_candles(
                Arc::clone(&pool),
                pair,
                &text,
                start,
                end,
            ).await {
                eprintln!("Ошибка агрегации для пары {}: {}", pair, e);
            }
        }
    }
}

pub async fn start_ws_trades(pool: Arc<PgPool>) {
    loop {
        let mut tasks = vec![]; 
        match connect_async(Url::parse(WS_URL).unwrap()).await {
            Ok((ws_stream, _)) => {
                println!("Подключено к WebSocket для трейдов!");

                let (write, mut read) = ws_stream.split();
                let write = Arc::new(Mutex::new(write));

                // Формируем запрос подписки на трейды
                let subscribe_request = json!({
                    "event": "subscribe",
                    "channel": ["trades"],
                    "symbols": ["BTC_USDT", "TRX_USDT", "ETH_USDT", "DOGE_USDT", "BCH_USDT"]
                });                

                {
                    let mut write_guard = write.lock().await;
                    if let Err(e) = write_guard.send(Message::Text(subscribe_request.to_string())).await {
                        eprintln!("Ошибка подписки на трейды: {}", e);
                        continue;
                    }
                }

                println!("Запрос на подписку на трейды отправлен!");

                // Фоновая задача для отправки PING
                let write_clone = Arc::clone(&write);
                tasks.push(tokio::spawn(async move {
                    let mut ping_interval = interval(Duration::from_secs(29));
                    loop {
                        ping_interval.tick().await;
                        let mut write_guard = write_clone.lock().await;
                        if let Err(e) = write_guard.send(Message::Text(json!({"event": "ping"}).to_string())).await {
                            eprintln!("Ошибка отправки PING в трейдах: {}", e);
                            break;
                        } else {
                            println!("PING (trades) отправлен");
                        }
                    }
                }));

                // Фоновая задача для обработки входящих трейд-сообщений
                let pool_clone = Arc::clone(&pool);
                tasks.push(tokio::spawn(async move {
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                //println!("Трейд-сообщение получено: {}", text);
                                if let Some(trade) = parse_trade_message(&text) {
                                    if let Err(e) = db::insert_trade(&pool_clone, trade).await {
                                        eprintln!("Ошибка записи трейда в БД: {}", e);
                                    } else {
                                        //println!("Трейд записан в БД.");
                                    }
                                }
                            }
                            Ok(Message::Binary(bin)) => {
                                println!("Бинарное сообщение (trades): {:?}", bin);
                            }
                            Ok(_) => {},
                            Err(e) => {
                                eprintln!("Ошибка WebSocket (trades): {}", e);
                                break;
                            }
                        }
                    }
                }));
                tasks.push(tokio::spawn(agg_candles(
                    Arc::clone(&pool),
                    "1m",
                    1,
                )));
                tasks.push(tokio::spawn(agg_candles(
                    Arc::clone(&pool),
                    "15m",
                    15,
                )));
                tasks.push(tokio::spawn(agg_candles(
                    Arc::clone(&pool),
                    "1h",
                    60,
                )));
                tasks.push(tokio::spawn(agg_candles(
                    Arc::clone(&pool),
                    "1d",
                    24*60,
                )));
                futures_util::future::join_all(tasks).await;
            }
            Err(e) => eprintln!("Ошибка подключения к WS для трейдов: {}", e),
        }

        println!("Переподключение трейдов через 5 секунд...");
        sleep(Duration::from_secs(5)).await;
    }
}

fn parse_trade_message(text: &str) -> Option<RecentTrade> {
    let parsed: Value = serde_json::from_str(text).ok()?;
    let data_array = parsed.get("data")?.as_array()?;
    let row = data_array.get(0)?;
    
    let pair = row.get("symbol")?.as_str()?.to_string();
    let price = row.get("price")?.as_str()?.to_string();
    let amount = row.get("amount")?.as_str()?.to_string();
    let quantity = row.get("quantity")?.as_str()?.to_string();
    let side = row.get("takerSide")?.as_str()?.to_string();
    let create_time = row.get("createTime")?.as_i64()?;
    let tid = match row.get("id") {
        Some(val) => {
            if let Some(s) = val.as_str() {
                s.to_string()
            } else if let Some(n) = val.as_i64() {
                n.to_string()
            } else {
                return None;
            }
        },
        None => return None,
    };
    let timestamp = row.get("ts")?.as_i64()?;
    
    Some(RecentTrade {
        tid,
        pair,
        price,
        amount,
        quantity,
        side,
        create_time,
        timestamp,
    })
}

pub async fn aggregate_trades_to_candles(
    pool: Arc<PgPool>,
    pair: &str,
    time_frame: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<(), sqlx::Error> {
    println!(
        "Выполняем агрегацию для пары '{}' с start_ts={} и end_ts={}",
        pair, start_ts, end_ts
    );
    
    let query = r#"
    WITH grouped AS (
      SELECT
        pair,
        date_trunc('minute', to_timestamp(time_stamp/1000.0)) AS utc_begin,
        MIN(time_stamp) AS first_ts,
        MAX(time_stamp) AS last_ts,
        MIN(price::numeric)::float8 AS low,
        MAX(price::numeric)::float8 AS high,
        SUM(CASE WHEN side = 'buy' THEN quantity::numeric ELSE 0 END)::float8 AS buy_base,
        SUM(CASE WHEN side = 'sell' THEN quantity::numeric ELSE 0 END)::float8 AS sell_base,
        SUM(CASE WHEN side = 'buy' THEN amount::numeric ELSE 0 END)::float8 AS buy_quote,
        SUM(CASE WHEN side = 'sell' THEN amount::numeric ELSE 0 END)::float8 AS sell_quote
      FROM trades
      WHERE pair = $1
        AND time_stamp BETWEEN $2 AND $3
      GROUP BY pair, date_trunc('minute', to_timestamp(time_stamp/1000.0))
    )
    SELECT
        g.pair,
        (SELECT price::numeric::float8
            FROM trades
            WHERE time_stamp = g.first_ts
            LIMIT 1
        ) AS o,
        g.high AS h,
        g.low AS l,
        (SELECT price::numeric::float8
            FROM trades
            WHERE time_stamp = g.last_ts
            LIMIT 1
        ) AS c,
        CAST(EXTRACT(EPOCH FROM g.utc_begin)*1000 AS float8) AS utc_begin_ms,
        g.buy_base,
        g.sell_base,
        g.buy_quote,
        g.sell_quote
    FROM grouped g;
    "#;
    
    let rows = sqlx::query(query)
        .bind(pair)
        .bind(start_ts)
        .bind(end_ts)
        .fetch_all(&*pool)
        .await?;
    
    println!("Получено {} строк", rows.len());
    for (i, row) in rows.iter().enumerate() {
        let o: Result<f64, _> = row.try_get("o");
        let h: Result<f64, _> = row.try_get("h");
        println!("Строка {}: o={:?}, h={:?}", i, o, h);
    }
    
    let mut candles = Vec::new();
    for row in rows {
        let pair_val: String = pair.to_string();
        let open: f64 = match row.try_get("o") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'o' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        let high: f64 = match row.try_get("h") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'h' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        let low: f64 = match row.try_get("l") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'l' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        let close: f64 = match row.try_get("c") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'c' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        let utc_begin_ms: f64 = match row.try_get("utc_begin_ms") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'utc_begin_ms' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        let buy_base: f64 = match row.try_get("buy_base") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'buy_base' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        let sell_base: f64 = match row.try_get("sell_base") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'sell_base' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        let buy_quote: f64 = match row.try_get("buy_quote") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'buy_quote' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        let sell_quote: f64 = match row.try_get("sell_quote") {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Ошибка получения 'sell_quote' для пары {}: {}", pair, e);
                return Err(e);
            }
        };
        
        let utc_begin = utc_begin_ms as i64;
        
        let volume_bs = VBS {
            buy_base,
            sell_base,
            buy_quote,
            sell_quote,
        };
        
        let candle = Kline {
            pair: pair_val,
            time_frame: time_frame.to_string(),
            open,
            high,
            low,
            close,
            volume_bs,
            utc_begin,
        };
        
        candles.push(candle);
    }
    
    if candles.is_empty() {
        println!(
            "Нет данных для агрегации: pair={}, time_frame={}, start_ts={}, end_ts={}",
            pair, time_frame, start_ts, end_ts
        );
        return Ok(());
    }
    
    db::insert_candles(&pool, candles).await?;
    println!(
        "Добавлено свечей для pair={}, time_frame={}, start_ts={}, end_ts={}",
        pair, time_frame, start_ts, end_ts
    );
    Ok(())
}