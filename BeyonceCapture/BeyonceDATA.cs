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
        private static readonly IMongoCollection<BsonDocument> BTCdeltasColl = db.GetCollection<BsonDocument>("BTCdeltasTEST");

        public static void StartDataUpdates()
        {
            //Begin Dequeue Thread:
            var DequeueThread = new Thread(() => ProcessQueue());
            DequeueThread.IsBackground = true;
            DequeueThread.Name = "Update-Dequeue-Thread";
            DequeueThread.Start();
        }

        public static void ProcessQueue()
        {
            
            //IMongoCollection<BsonDocument> ETHdeltasColl = db.GetCollection<BsonDocument>("BTCdeltas");
            //IMongoCollection<BsonDocument> BNBdeltasColl = db.GetCollection<BsonDocument>("BTCdeltas");

            InsertManyOptions options = new InsertManyOptions()
            {               
                IsOrdered = false
            };

            var documents = new List<BsonDocument>();

            while (true)
            {
                if (UpdateQueue.IsEmpty)
                {
                    // no pending updates available pause
                    if (UpdateQueue.Count == 0 && documents.Count > 20)
                    {
                        BTCdeltasColl.InsertManyAsync(documents, options).Wait();
                        Console.WriteLine($"1Wrote docs: {documents.Count}");
                        documents = new List<BsonDocument>();
                    }

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


                        var bsonMsg = BsonSerializer.Deserialize<BsonDocument>(msg);
                        documents.Add(bsonMsg);


                    }
                } while (!tryDQ);


                if (documents.Count > 80)
                {
                    BTCdeltasColl.InsertManyAsync(documents, options).Wait();
                    Console.WriteLine($"2Wrote docs: {documents.Count}");
                    documents = new List<BsonDocument>();
                }







            }
        }
    }

    
}
