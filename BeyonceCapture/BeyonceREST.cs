using System;
using System.Linq;
using System.Net.Http;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Bson;
using PureWebSockets;
using Utf8Json;

namespace BeyonceCapture
{
    
    public static class BeyonceREST
    {
        private static HttpClient client = new HttpClient()
        {
            BaseAddress = new Uri("https://api.binance.com/"),
        };

        public static async Task<MarketDepthSnapshot> GetDepthSnapshot(string delta)
        {
            var uri = $"/api/v1/depth?symbol={delta.ToUpper()}&limit=1000";
         
            HttpResponseMessage response = await client.GetAsync(uri);
            if (response.IsSuccessStatusCode)
            {
                var stream = await response.Content.ReadAsStreamAsync();
                return await JsonSerializer.DeserializeAsync<MarketDepthSnapshot>(stream);
            }
            else
                return null;
        }

        public static async Task<ExchangeInfo> GetMarketSummary()
        {
            var uri = "/api/v1/exchangeInfo";

            HttpResponseMessage response = await client.GetAsync(uri);
            if (response.IsSuccessStatusCode)
            {
                var stream = await response.Content.ReadAsStreamAsync();
                return await JsonSerializer.DeserializeAsync<ExchangeInfo>(stream);
            }
            else
                return null;
        }

        public static async Task<List<Info24h>> Get24hInfo()
        {
            var uri = "/api/v1/ticker/24hr";

            HttpResponseMessage response = await client.GetAsync(uri);
            if (response.IsSuccessStatusCode)
            {
                var stream = await response.Content.ReadAsStreamAsync();
                return await JsonSerializer.DeserializeAsync<List<Info24h>>(stream);
            }
            else
                return null;
        }


        public static async Task<Info24h> Get24hInfo(string delta)
        {
            var uri = $"/api/v1/ticker/24hr?symbol={delta.ToUpper()}";

            HttpResponseMessage response = await client.GetAsync(uri);
            if (response.IsSuccessStatusCode)
            {
                var stream = await response.Content.ReadAsStreamAsync();
                return await JsonSerializer.DeserializeAsync<Info24h>(stream);
            }
            else
                return null;
        }

    }
}

