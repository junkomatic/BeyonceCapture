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
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            MainAsync().Wait();

            Console.WriteLine("\t-PRESS ENTER 3 TIMES TO EXIT-");
            Console.ReadLine();
            Console.ReadLine();
            Console.ReadLine();
            Environment.Exit(0);
        }


        static async Task MainAsync()
        {
            //var connectionString = "mongodb://localhost:27017";
            //var client = new MongoClient(connectionString);
            //IMongoDatabase db = client.GetDatabase("marketData");
            //IMongoCollection<BsonDocument> BTCdeltasColl = db.GetCollection<BsonDocument>("BTCdeltas");
            //IMongoCollection<BsonDocument> ETHdeltasColl = db.GetCollection<BsonDocument>("BTCdeltas");
            //IMongoCollection<BsonDocument> BNBdeltasColl = db.GetCollection<BsonDocument>("BTCdeltas");
            
            
            var infos = await BeyonceREST.Get24hInfo();

            var BTCdeltas = from info in infos
                            where info.symbol.Substring(info.symbol.Length - 3, 3) == "BTC"
                            orderby info.quoteVolume descending
                            select info.symbol;

            //var ETHdeltas = from info in infos
            //                where info.symbol.Substring(info.symbol.Length - 3, 3) == "ETH"
            //                orderby info.quoteVolume
            //                select info.symbol;

            //var BNBdeltas = from info in infos
            //                where info.symbol.Substring(info.symbol.Length - 3, 3) == "BNB"
            //                orderby info.quoteVolume
            //                select info.symbol;

            //var USDTdeltas = from info in infos
            //                 where info.symbol.Substring(info.symbol.Length - 4, 4) == "USDT"
            //                 orderby info.quoteVolume
            //                 select info.symbol;


            MarketSocket socket = new MarketSocket(BTCdeltas.Take(20).ToList());
            socket.Connect();
            MarketSocket socket2 = new MarketSocket(BTCdeltas.Skip(20).Take(20).ToList());
            socket2.Connect();
            MarketSocket socket3 = new MarketSocket(BTCdeltas.Skip(40).Take(20).ToList());
            socket3.Connect();


            Console.ReadLine();


        }




    }
    

}
