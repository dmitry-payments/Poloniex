use chrono::{Utc, TimeZone};
use crate::data_structs::{Kline, VBS};                    

pub fn get_time_range() -> (i64, i64) {
    let start_time = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0)
        .unwrap()
        .timestamp_millis();
    let end_time = Utc::now().timestamp_millis();
    (start_time, end_time)
}

pub async fn get_candles(symbol: &str, interval: &str) -> Result<Vec<Kline>, Box<dyn std::error::Error>> {
    let (start_time, end_time) = get_time_range();

    let url = format!(
        "https://api.poloniex.com/markets/{}/candles?interval={}&startTime={}&endTime={}",
        symbol, interval, start_time, end_time
    );

    let response = reqwest::get(&url).await?;
    let text = response.text().await?;

    println!("Ответ API: {}", text);

    let data: Vec<Vec<serde_json::Value>> = serde_json::from_str(&text)
        .map_err(|e| {
            eprintln!("Ошибка парсинга JSON: {}", e);
            Box::new(e) as Box<dyn std::error::Error>
        })?;

    println!("Длина массива: {}", data.len());

    let candles: Vec<Kline> = data.into_iter().map(|row| {
        // Согласно спецификации:
        // index 0: low (string)
        // index 1: high (string)
        // index 2: open (string)
        // index 3: close (string)
        // index 4: amount (quote volume, string)
        // index 5: quantity (base volume, string)
        // index 6: buyTakerAmount (string)
        // index 7: buyTakerQuantity (string)
        // index 8: tradeCount (integer) – не используем
        // index 9: ts (integer) – не используем
        // index 10: weightedAverage (string) – не используем
        // index 11: interval (string) – не используем, берём из аргумента
        // index 12: startTime (integer) – используем как utc_begin
        // index 13: closeTime (integer) – не используем

        let low = row.get(0)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_default();
        let high = row.get(1)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_default();
        let open = row.get(2)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_default();
        let close = row.get(3)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_default();

        let amount = row.get(4)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_default();
        let quantity = row.get(5)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_default();
        let buy_taker_amount = row.get(6)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_default();
        let buy_taker_quantity = row.get(7)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_default();
        let utc_begin = row.get(12)
            .and_then(|v| v.as_i64())
            .unwrap_or_default();

        let volume_bs = VBS {
            buy_base: buy_taker_quantity,
            sell_base: quantity - buy_taker_quantity,
            buy_quote: buy_taker_amount,
            sell_quote: amount - buy_taker_amount,
        };

        Kline {
            pair: symbol.to_string(),
            time_frame: interval.to_string(),
            open,
            high,
            low,
            close,
            volume_bs,
            utc_begin,
        }
    }).collect();

    println!("Преобразованные свечи: {:?}", candles);

    Ok(candles)
}

