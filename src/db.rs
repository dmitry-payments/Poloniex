use sqlx::postgres::PgPool;
use sqlx::Error;
use crate::data_structs::Kline;
use crate::data_structs::RecentTrade;

use sqlx::postgres::PgArguments;
use sqlx::Arguments;

pub async fn insert_candles(pool: &PgPool, candles: Vec<Kline>) -> Result<(), sqlx::Error> {
    if candles.is_empty() {
        return Ok(());
    }

    let mut query = String::from("INSERT INTO candles 
        (pair, time_frame, open, high, low, close, buy_base, sell_base, buy_quote, sell_quote, utc_begin)
        VALUES ");
    
    let mut args = PgArguments::default();
    let mut placeholders = vec![];

    for (i, candle) in candles.iter().enumerate() {
        let offset = i * 11;
        placeholders.push(format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
            offset + 1,  // pair
            offset + 2,  // time_frame
            offset + 3,  // open
            offset + 4,  // high
            offset + 5,  // low
            offset + 6,  // close
            offset + 7,  // buy_base
            offset + 8,  // sell_base
            offset + 9,  // buy_quote
            offset + 10, // sell_quote
            offset + 11  // utc_begin
        ));

        args.add(&candle.pair);
        args.add(&candle.time_frame);
        args.add(candle.open);
        args.add(candle.high);
        args.add(candle.low);
        args.add(candle.close);
        args.add(candle.volume_bs.buy_base);
        args.add(candle.volume_bs.sell_base);
        args.add(candle.volume_bs.buy_quote);
        args.add(candle.volume_bs.sell_quote);
        args.add(candle.utc_begin);
    }

    query.push_str(&placeholders.join(", "));
    sqlx::query_with(&query, args).execute(pool).await?;

    Ok(())
}

pub async fn insert_trade(pool: &PgPool, trade: RecentTrade) -> Result<(), Error> {

    let query = "
    INSERT INTO trades (tid, pair, amount, side, quantity, create_time, price, time_stamp)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (tid) DO NOTHING;
    ";

    sqlx::query(query)
    .bind(trade.tid)       
    .bind(trade.pair)       
    .bind(trade.amount)    
    .bind(trade.side)  
    .bind(trade.quantity)
    .bind(trade.create_time)
    .bind(trade.price)
    .bind(trade.timestamp)
    .execute(pool)
    .await?;

    Ok(())
}
