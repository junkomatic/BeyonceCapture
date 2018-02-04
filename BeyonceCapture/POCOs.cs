using System;
using System.Collections.Generic;
using System.Text;

namespace BeyonceCapture
{

    public class streamSubjectLine
    {
        public string stream { get; set; }
    }

   public class DepthMsgData
    {
        public string e { get; set; }
        public long E { get; set; }
        public string s { get; set; }
        public int U { get; set; }
        public int u { get; set; }
        public List<List<object>> b { get; set; }
        public List<List<object>> a { get; set; }
    }
    
    public class MarketDepthJSON
    {
        public string stream { get; set; }
        public DepthMsgData data { get; set; }
    }

    public class TradeMsgData
    {
        public string e { get; set; }
        public long E { get; set; }
        public string s { get; set; }
        public int t { get; set; }
        public string p { get; set; }
        public string q { get; set; }
        public int b { get; set; }
        public int a { get; set; }
        public long T { get; set; }
        public bool m { get; set; }
        public bool M { get; set; }
    }

    public class MarketTradeJSON
    {
        public string stream { get; set; }
        public TradeMsgData data { get; set; }

    }

    public class MarketDepthSnapshot
    {
        public int lastUpdateId { get; set; }
        public List<List<object>> bids { get; set; }
        public List<List<object>> asks { get; set; }
    }




    public class RateLimit
    {
        public string rateLimitType { get; set; }
        public string interval { get; set; }
        public int limit { get; set; }
    }

    public class Filter
    {
        public string filterType { get; set; }
        public string minPrice { get; set; }
        public string maxPrice { get; set; }
        public string tickSize { get; set; }
        public string minQty { get; set; }
        public string maxQty { get; set; }
        public string stepSize { get; set; }
        public string minNotional { get; set; }
    }

    public class Symbol
    {
        public string symbol { get; set; }
        public string status { get; set; }
        public string baseAsset { get; set; }
        public int baseAssetPrecision { get; set; }
        public string quoteAsset { get; set; }
        public int quotePrecision { get; set; }
        public List<string> orderTypes { get; set; }
        public bool icebergAllowed { get; set; }
        public List<Filter> filters { get; set; }
    }

    public class ExchangeInfo
    {
        public string timezone { get; set; }
        public long serverTime { get; set; }
        public List<RateLimit> rateLimits { get; set; }
        public List<object> exchangeFilters { get; set; }
        public List<Symbol> symbols { get; set; }
    }


    public class Info24h
    {
        public string symbol { get; set; }
        public string priceChange { get; set; }
        public string priceChangePercent { get; set; }
        public string weightedAvgPrice { get; set; }
        public string prevClosePrice { get; set; }
        public string lastPrice { get; set; }
        public string lastQty { get; set; }
        public string bidPrice { get; set; }
        public string bidQty { get; set; }
        public string askPrice { get; set; }
        public string askQty { get; set; }
        public string openPrice { get; set; }
        public string highPrice { get; set; }
        public string lowPrice { get; set; }
        public string volume { get; set; }
        public string quoteVolume { get; set; }
        public object openTime { get; set; }
        public object closeTime { get; set; }
        public int firstId { get; set; }
        public int lastId { get; set; }
        public int count { get; set; }
    }

}
