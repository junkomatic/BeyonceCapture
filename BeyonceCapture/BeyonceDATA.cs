using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using Utf8Json;

namespace BeyonceCapture
{
    public static class BeyonceDATA
    {
        public static ConcurrentQueue<string> UpdateQueue = new ConcurrentQueue<string>();
        private static readonly MongoClient client = new MongoClient("mongodb://localhost:27017");
        private static readonly IMongoDatabase db = client.GetDatabase("marketData");

        private static IMongoCollection<BsonDocument> BTCpairsCollection;
        private static IMongoCollection<BsonDocument> ETHpairsCollection;
        private static IMongoCollection<BsonDocument> BNBpairsCollection;
        private static IMongoCollection<BsonDocument> USDTpairsCollection;
        private static List<BsonDocument> BTCdocuments;
        private static List<BsonDocument> ETHdocuments;
        private static List<BsonDocument> BNBdocuments;
        private static List<BsonDocument> USDTdocuments;

        public static async void StartDataUpdates()
        {
            BTCpairsCollection = await CheckCreateGetCollection("BTCmarketsTEST");
            ETHpairsCollection = await CheckCreateGetCollection("ETHmarketsTEST");
            BNBpairsCollection = await CheckCreateGetCollection("BNBmarketsTEST");
            USDTpairsCollection = await CheckCreateGetCollection("USDTmarketsTEST");

            BTCdocuments = new List<BsonDocument>();
            ETHdocuments = new List<BsonDocument>();
            BNBdocuments = new List<BsonDocument>();
            USDTdocuments = new List<BsonDocument>();


            //Begin Dequeue Thread:
            var DequeueThread = new Thread(() => ProcessQueue());
            DequeueThread.IsBackground = true;
            DequeueThread.Name = "Update-Dequeue-Thread";
            DequeueThread.Start();
        }

        public static void ProcessQueue()
        {            
            while (true)
            {
                if (UpdateQueue.IsEmpty)
                {
                    // no pending updates available pause
                    SendAllData();
                    Thread.Sleep(100);
                    continue;
                }
            
                bool tryDQ = false;
                do
                {
                    string msg;
                    tryDQ = UpdateQueue.TryDequeue(out msg);
                    if (tryDQ)
                    {
                        var msgSplit = msg.Split('@');

                        var msgType = msgSplit[1].Substring(0, 5);
                        var quoteSymbol = msgSplit[0].Substring(msgSplit[0].Length - 3, 3);
                        var BSONdocument = new BsonDocument();

                        if (msgType == "depth")                        
                            BSONdocument = CreateAddDepthBSON(JsonSerializer.Deserialize<MarketDepthMsg>(msg));                        
                        else if (msgType == "trade")
                            BSONdocument = CreateAddTradeBSON(JsonSerializer.Deserialize<MarketTradeMsg>(msg));
                        
                        AddBSONdoc(quoteSymbol, BSONdocument);
                    }
                } while (!tryDQ);
                
                SendAllData(80);
            }
        }
        

        private static BsonDocument CreateAddDepthBSON(MarketDepthMsg depthJSON)
        {
            return new BsonDocument()
            {
                //TODO: ID = TIMESTAMP
                //      TRADE AND DEPTH AND SNAP SCHEMA


            };

        }

        private static BsonDocument CreateAddTradeBSON(MarketTradeMsg tradeJSON)
        {
            return new BsonDocument()
            {
                //TODO: ID = TIMESTAMP
                //      TRADE AND DEPTH AND SNAP SCHEMA


            };
            
        }
        

        public static void CreateAddSnapshotDoc(object TODO)
        {
            //TODO: GET QUOTE SYMBOL, CREATE DOC
            var quoteSymbol = "";
            var BSONdoc = new BsonDocument();




            AddBSONdoc(quoteSymbol, BSONdoc);
        }


        private static void AddBSONdoc(string symbol, BsonDocument msgBSON)
        {
            switch (symbol)
            {
                case "btc":
                    BTCdocuments.Add(msgBSON);
                    break;
                case "eth":
                    ETHdocuments.Add(msgBSON);
                    break;
                case "bnb":
                    BNBdocuments.Add(msgBSON);
                    break;
                case "sdt":
                    USDTdocuments.Add(msgBSON);
                    break;
            }
        }


        private static void SendAllData(int docCount = 0)
        {
            SendBTCdata(docCount);
            SendETHdata(docCount);
            SendBNBdata(docCount);
            SendUSDTdata(docCount);
        }  
        
        private static void SendBTCdata(int docCount = 0)
        {
            if (BTCdocuments.Count > docCount)
            {
                BTCpairsCollection.InsertManyAsync(BTCdocuments, new InsertManyOptions() { IsOrdered = false }).Wait();
                Console.WriteLine($"BTC docs: {BTCdocuments.Count}");
                BTCdocuments = new List<BsonDocument>();
            }
        }
        private static void SendETHdata(int docCount = 0)
        {
            if (ETHdocuments.Count > docCount)
            {
                ETHpairsCollection.InsertManyAsync(ETHdocuments, new InsertManyOptions() { IsOrdered = false }).Wait();
                Console.WriteLine($"ETH docs: {ETHdocuments.Count}");
                ETHdocuments = new List<BsonDocument>();
            }
        }
        private static void SendBNBdata(int docCount = 0)
        {
            if (BNBdocuments.Count > docCount)
            {
                BNBpairsCollection.InsertManyAsync(BNBdocuments, new InsertManyOptions() { IsOrdered = false }).Wait();
                Console.WriteLine($"BNB docs: {BNBdocuments.Count}");
                BNBdocuments = new List<BsonDocument>();
            }
        }
        private static void SendUSDTdata(int docCount = 0)
        {
            if (USDTdocuments.Count > docCount)
            {
                USDTpairsCollection.InsertManyAsync(USDTdocuments, new InsertManyOptions() { IsOrdered = false }).Wait();
                Console.WriteLine($"USDT docs: {USDTdocuments.Count}");
                USDTdocuments = new List<BsonDocument>();
            }
        }


        private static async Task<IMongoCollection<BsonDocument>> CheckCreateGetCollection(string quoteSymbol)
        {
            var symbol = quoteSymbol.ToUpper();
            var filter = new BsonDocument("name", $"{symbol}markets");
            //filter by collection name
            var collections = await db.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
            //check for existence, create if !exist
            if (!await collections.AnyAsync())
            {
                var options = new CreateCollectionOptions() { StorageEngine = BsonDocument.Parse("{wiredTiger:{configString:'block_compressor=zlib'}}") };
                db.CreateCollection($"{symbol}markets", options);
            }

            return db.GetCollection<BsonDocument>($"{symbol}markets");            
        }



    }

    
}
