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
            var exists = await CollectionExistsAsync("BTCmarketsTEST");
            if (!exists)
            {
                var options = new CreateCollectionOptions() { StorageEngine = BsonDocument.Parse("{wiredTiger:{configString:'block_compressor=zlib'}}") };
                db.CreateCollection("BTCdeltasTEST", options);
            }
            BTCpairsCollection = db.GetCollection<BsonDocument>("BTCmarkets");
            ETHpairsCollection = db.GetCollection<BsonDocument>("ETHmarkets");
            BNBpairsCollection = db.GetCollection<BsonDocument>("BNBmarkets");
            USDTpairsCollection = db.GetCollection<BsonDocument>("USDTmarkets");

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
                        //TODO: SORT MESSAGES, 
                        //      ID = TIMESTAMP
                        //      TRADE AND DEPTH SCHEMA?
                       

                        var bsonMsg = BsonSerializer.Deserialize<BsonDocument>(msg);
                        //BTCdocuments.Add(bsonMsg);


                    }
                } while (!tryDQ);


                if (BTCdocuments.Count > 80 || ETHdocuments.Count > 80 || BNBdocuments.Count > 80 || USDTdocuments.Count > 80)
                {
                    SendAllData();
                }

            }
        }


        private static void SendAllData()
        {
            SendBTCdata();
            SendETHdata();
            SendBNBdata();
            SendUSDTdata();
        }


        private static void SendBTCdata()
        {
            if (BTCdocuments.Count > 0)
            {
                BTCpairsCollection.InsertManyAsync(BTCdocuments, new InsertManyOptions() { IsOrdered = false }).Wait();
                Console.WriteLine($"BTC docs: {BTCdocuments.Count}");
                BTCdocuments = new List<BsonDocument>();
            }
        }
        private static void SendETHdata()
        {
            if (ETHdocuments.Count > 0)
            {
                ETHpairsCollection.InsertManyAsync(ETHdocuments, new InsertManyOptions() { IsOrdered = false }).Wait();
                Console.WriteLine($"ETH docs: {ETHdocuments.Count}");
                ETHdocuments = new List<BsonDocument>();
            }
        }
        private static void SendBNBdata()
        {
            if (BNBdocuments.Count > 0)
            {
                BNBpairsCollection.InsertManyAsync(BNBdocuments, new InsertManyOptions() { IsOrdered = false }).Wait();
                Console.WriteLine($"BNB docs: {BNBdocuments.Count}");
                BNBdocuments = new List<BsonDocument>();
            }
        }
        private static void SendUSDTdata()
        {
            if (USDTdocuments.Count > 0)
            {
                USDTpairsCollection.InsertManyAsync(USDTdocuments, new InsertManyOptions() { IsOrdered = false }).Wait();
                Console.WriteLine($"USDT docs: {USDTdocuments.Count}");
                USDTdocuments = new List<BsonDocument>();
            }
        }


        private static async Task<bool> CollectionExistsAsync(string collectionName)
        {
            var filter = new BsonDocument("name", collectionName);
            //filter by collection name
            var collections = await db.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
            //check for existence
            return await collections.AnyAsync();
        }

    }

    
}
