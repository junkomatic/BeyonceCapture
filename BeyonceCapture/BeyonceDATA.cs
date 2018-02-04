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
        private static List<UpdateOneModel<BsonDocument>> BTCupserts;
        private static List<UpdateOneModel<BsonDocument>> ETHupserts;
        private static List<UpdateOneModel<BsonDocument>> BNBupserts;
        private static List<UpdateOneModel<BsonDocument>> USDTupserts;

        public static async void StartDataUpdates()
        {
            BTCpairsCollection = await CheckCreateGetCollection("BTCmarketsTEST");
            ETHpairsCollection = await CheckCreateGetCollection("ETHmarketsTEST");
            BNBpairsCollection = await CheckCreateGetCollection("BNBmarketsTEST");
            USDTpairsCollection = await CheckCreateGetCollection("USDTmarketsTEST");

            BTCupserts = new List<UpdateOneModel<BsonDocument>>();
            ETHupserts = new List<UpdateOneModel<BsonDocument>>();
            BNBupserts = new List<UpdateOneModel<BsonDocument>>();
            USDTupserts = new List<UpdateOneModel<BsonDocument>>();


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
                        
                        if (msgType == "depth")
                            AddDataUpsert(quoteSymbol, CreateDepthUpsert(JsonSerializer.Deserialize<MarketDepthMsg>(msg), msgSplit[0]));                        
                        else if (msgType == "trade")
                            AddDataUpsert(quoteSymbol, CreateTradeUpsert(JsonSerializer.Deserialize<MarketTradeMsg>(msg), msgSplit[0]));
                        
                    }
                } while (!tryDQ);
                
                SendAllData(80);
            }
        }
        



        private static UpdateOneModel<BsonDocument> CreateDepthUpsert(MarketDepthMsg depthJSON, string delta)
        {
            var BSONdoc = new BsonDocument()
            {
                //TODO:CREATE DOC DEF





            };
            
            var filter = Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "time")
                & Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "pair");

            return new UpdateOneModel<BsonDocument>(filter, BSONdoc) { IsUpsert = true };
        }

        private static UpdateOneModel<BsonDocument> CreateTradeUpsert(MarketTradeMsg tradeJSON, string delta)
        {
            var BSONdoc = new BsonDocument()
            {
                //TODO: CREATE DOC DEF





            };

            var filter = Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "time")
                & Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "pair");
            
            return new UpdateOneModel<BsonDocument>(filter, BSONdoc) { IsUpsert = true };
        }
        

        public static void CreateAddSnapshotDoc(object TODO, string quoteSymbol)
        {
            var BSONdoc = new BsonDocument()
            {
                //TODO: CREATE DOC DEF
                //FILTER WILL USE LAST-UPDATE-ID: u/U




            };
            
            var filter = Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "pair")
                & Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "TODO__UPDATE_ID!!");
            
            var upsert = new UpdateOneModel<BsonDocument>(filter, BSONdoc) { IsUpsert = true };
            AddDataUpsert(quoteSymbol, upsert);
        }


        private static void AddDataUpsert(string symbol, UpdateOneModel<BsonDocument> msgBSON)
        {
            switch (symbol)
            {
                case "btc":
                    BTCupserts.Add(msgBSON);
                    break;
                case "eth":
                    ETHupserts.Add(msgBSON);
                    break;
                case "bnb":
                    BNBupserts.Add(msgBSON);
                    break;
                case "sdt":
                    USDTupserts.Add(msgBSON);
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
            if (BTCupserts.Count > docCount)
            {   
                BTCpairsCollection.BulkWriteAsync(BTCupserts, new BulkWriteOptions() { IsOrdered = false }).Wait();

                Console.WriteLine($"BTC docs: {BTCupserts.Count}");
                BTCupserts = new List<UpdateOneModel<BsonDocument>>();
            }
        }
        private static void SendETHdata(int docCount = 0)
        {
            if (ETHupserts.Count > docCount)
            {
                ETHpairsCollection.BulkWriteAsync(BTCupserts, new BulkWriteOptions() { IsOrdered = false }).Wait();

                Console.WriteLine($"ETH docs: {ETHupserts.Count}");
                ETHupserts = new List<UpdateOneModel<BsonDocument>>();
            }
        }
        private static void SendBNBdata(int docCount = 0)
        {
            if (BNBupserts.Count > docCount)
            {
                BNBpairsCollection.BulkWriteAsync(BTCupserts, new BulkWriteOptions() { IsOrdered = false }).Wait();

                Console.WriteLine($"BNB docs: {BNBupserts.Count}");
                BNBupserts = new List<UpdateOneModel<BsonDocument>>();
            }
        }
        private static void SendUSDTdata(int docCount = 0)
        {
            if (USDTupserts.Count > docCount)
            {
                USDTpairsCollection.BulkWriteAsync(BTCupserts, new BulkWriteOptions() { IsOrdered = false }).Wait();

                Console.WriteLine($"USDT docs: {USDTupserts.Count}");
                USDTupserts = new List<UpdateOneModel<BsonDocument>>();
            }
        }


        private static async Task<IMongoCollection<BsonDocument>> CheckCreateGetCollection(string quoteSymbol)
        {
            //CREATES A 'zlib' COMPRESSED COLLECTION, IF DOESNT EXIST
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
