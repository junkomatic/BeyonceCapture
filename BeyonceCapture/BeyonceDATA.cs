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
                            AddDataUpsert(quoteSymbol, CreateDepthUpsert(JsonSerializer.Deserialize<MarketDepthJSON>(msg), msgSplit[0]));                        
                        else if (msgType == "trade")
                            AddDataUpsert(quoteSymbol, CreateTradeUpsert(JsonSerializer.Deserialize<MarketTradeJSON>(msg), msgSplit[0]));
                        
                    }
                } while (!tryDQ);
                
                SendAllData(80);
            }
        }
        



        private static UpdateOneModel<BsonDocument> CreateDepthUpsert(MarketDepthJSON depthJSON, string delta)
        {
            //FORM ASKS ARRAY
            var ASKSarray = new BsonArray();
            var index = 0;
            foreach (List<object> ask in depthJSON.data.a)
            {
                var ASKdoc = new BsonDocument
                {
                    { "rate", BsonValue.Create(depthJSON.data.a[index][0]) },
                    { "qty", BsonValue.Create(depthJSON.data.a[index][1]) }
                };

                ASKSarray.Add(ASKdoc);
                index++;
            }

            //FORM BIDS ARRAY
            var BIDSarray = new BsonArray();
            index = 0;
            foreach (List<object> bid in depthJSON.data.b)
            {
                var BIDdoc = new BsonDocument
                {
                    { "rate", BsonValue.Create(depthJSON.data.b[index][0]) },
                    { "qty", BsonValue.Create(depthJSON.data.b[index][1]) }
                };

                BIDSarray.Add(BIDdoc);
                index++;
            }

            //DEFINE DOCUMENT FOR UPSERT
            var BSONdoc = new BsonDocument()
            {
                {"time", BsonValue.Create(depthJSON.data.E)},
                {"pair", new BsonString(delta)},
                { "depthData", new BsonDocument {
                    {"U", depthJSON.data.U},
                    {"u",  depthJSON.data.u},
                    { "asks", ASKSarray },
                    { "bids", BIDSarray }                        
                } }
            };
            
            //FILTER UPSERT TO MATCH time AND pair FOR ROOT DOCUMENT
            var filter = Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "time")
                & Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "pair");

            return new UpdateOneModel<BsonDocument>(filter, BSONdoc) { IsUpsert = true };
        }

        private static UpdateOneModel<BsonDocument> CreateTradeUpsert(MarketTradeJSON tradeJSON, string delta)
        {
            var BSONdoc = new BsonDocument()
            {
                //TODO: CREATE DOC DEF
                //THERE MAY BE MANY TRADE EVENTS PER TIMESTAMP






            };

            var filter = Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "time")
                & Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "pair");
            
            return new UpdateOneModel<BsonDocument>(filter, BSONdoc) { IsUpsert = true };
        }
        

        public static void CreateAddSnapshotDoc(object TODO, string delta)
        {
            var BSONdoc = new BsonDocument()
            {
                //TODO: CREATE DOC DEF
                //FILTER WILL USE LAST-UPDATE-ID: U/u




            };
            
            var filter = Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "pair")
                & Builders<BsonDocument>.Filter.ElemMatch(x => x.Elements, x => x.Name == "TODO__UPDATE_ID!!");
            
            var upsert = new UpdateOneModel<BsonDocument>(filter, BSONdoc) { IsUpsert = true };
            AddDataUpsert(delta.Substring(delta.Length - 3, 3), upsert);
        }


        private static void AddDataUpsert(string symbol, UpdateOneModel<BsonDocument> msgBSON)
        {
            switch (symbol.ToLower())
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
