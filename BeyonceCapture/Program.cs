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


            var sockets = new List<MarketSocket>();
            var socketCount = 0;
            var pages = Math.Ceiling(BTCdeltas.Count() / 5M);
            for (var page = 0; page <= pages; page++)
            {
                var subDeltas = BTCdeltas.Skip(5 * page).Take(5);
                //foreach (var delta in subDeltas)
                //{
                //    var deltaSocket = new MarketSocket(delta);
                //    deltaSocket.Connect();
                //    sockets.Add(deltaSocket);
                //}

                var socket = new MarketSocket(subDeltas.ToList());
                sockets.Add(socket);
                socketCount += subDeltas.Count();



                Console.WriteLine($"{page}/{pages}   ... socketCount: {sockets.Count}");
                Console.ReadLine();
            }


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

            //Console.WriteLine($"Total Count: {infos.Count}");
            //Console.WriteLine($"BTC deltas: {BTCdeltas.Count()}");
            //Console.WriteLine($"ETH deltas: {ETHdeltas.Count()}");
            //Console.WriteLine($"BNB deltas: {BNBdeltas.Count()}");
            //Console.WriteLine($"USDT deltas: {USDTdeltas.Count()}");



            MarketSocket ETHsocket = new MarketSocket("ethbtc");
            ETHsocket.Connect();





        }




    }
    

}
