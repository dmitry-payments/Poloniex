use tokio_tungstenite::connect_async;
use futures_util::{StreamExt, SinkExt};
use tokio::time::{sleep, interval, Duration};
use tokio_tungstenite::tungstenite::protocol::Message;
use serde_json::{json, Value};
use url::Url;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::data_structs::{Kline, VBS};
use crate::data_structs::RecentTrade;
use crate::db;
use sqlx::PgPool;

const WS_URL: &str = "wss://ws.poloniex.com/ws/public";

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

pub async fn start_ws_trades(pool: Arc<PgPool>) {
    loop {
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
                tokio::spawn(async move {
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
                });

                // Фоновая задача для обработки входящих трейд-сообщений
                let pool_clone = Arc::clone(&pool);
                tokio::spawn(async move {
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                println!("Трейд-сообщение получено: {}", text);
                                if let Some(trade) = parse_trade_message(&text) {
                                    if let Err(e) = db::insert_trade(&pool_clone, trade).await {
                                        eprintln!("Ошибка записи трейда в БД: {}", e);
                                    } else {
                                        println!("Трейд записан в БД.");
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
                });
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
    let amount = row.get("quantity")?.as_str()?.to_string();
    let side = row.get("takerSide")?.as_str()?.to_string();
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
        side,
        timestamp,
    })
}