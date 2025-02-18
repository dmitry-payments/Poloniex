pub const PAIRS: [&str; 5] = ["BTC_USDT", "TRX_USDT", "ETH_USDT", "DOGE_USDT", "BCH_USDT"];
pub const INTERVALS: [&str; 4] = ["MINUTE_1", "MINUTE_15", "HOUR_1", "DAY_1"];

#[derive(Debug)]
pub struct VBS {
    pub buy_base: f64,   // объём покупок в базовой валюте
    pub sell_base: f64,  // объём продаж в базовой валюте
    pub buy_quote: f64,  // объём покупок в котируемой валюте
    pub sell_quote: f64, // объём продаж в котируемой валюте
}

#[derive(Debug)]
pub struct Kline {
    pub pair: String,
    pub time_frame: String,
    pub open: f64,       // индекс 0
    pub high: f64,       // индекс 1
    pub low: f64,        // индекс 2
    pub close: f64,      // индекс 3
    pub volume_bs: VBS,  // вычисляемая структура
    pub utc_begin: i64,  // индекс 9
}

#[derive(Debug)]
pub struct RecentTrade {
    pub tid: String,      // id транзакции
    pub pair: String,     // название валютной пары (как у нас)
    pub price: String,    // цена транзакции
    pub amount: String,   // объём транзакции в базовой валюте
    pub side: String,     // как биржа засчитала эту сделку (buy или sell)
    pub timestamp: i64,   // время UTC UnixNano
}
