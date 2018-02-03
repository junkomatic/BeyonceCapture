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
           

            var infos = await BeyonceREST.Get24hInfo();

            var BTCdeltas = from info in infos
                            where info.symbol.Substring(info.symbol.Length - 3, 3) == "BTC"
                            orderby info.quoteVolume descending
                            select info.symbol;

            var ETHdeltas = from info in infos
                            where info.symbol.Substring(info.symbol.Length - 3, 3) == "ETH"
                            orderby info.quoteVolume descending
                            select info.symbol;

            //var BNBdeltas = from info in infos
            //                where info.symbol.Substring(info.symbol.Length - 3, 3) == "BNB"
            //                orderby info.quoteVolume descending
            //                select info.symbol;

            //var USDTdeltas = from info in infos
            //                 where info.symbol.Substring(info.symbol.Length - 4, 4) == "USDT"
            //                 orderby info.quoteVolume descending
            //                 select info.symbol;


            BeyonceDATA.StartDataUpdates();
            MarketSocket socket = new MarketSocket(BTCdeltas.Take(20).ToList());
            socket.Connect();
            MarketSocket socket2 = new MarketSocket(BTCdeltas.Skip(20).Take(20).ToList());
            socket2.Connect();
            MarketSocket socket3 = new MarketSocket(BTCdeltas.Skip(40).Take(20).ToList());
            socket3.Connect();
            MarketSocket socket4 = new MarketSocket(BTCdeltas.Skip(60).Take(20).ToList());
            socket4.Connect();
            MarketSocket socket5 = new MarketSocket(BTCdeltas.Skip(80).Take(20).ToList());
            socket5.Connect();


            MarketSocket socketE = new MarketSocket(ETHdeltas.Take(20).ToList());
            socketE.Connect();
            MarketSocket socket2E = new MarketSocket(ETHdeltas.Skip(20).Take(20).ToList());
            socket2E.Connect();
            MarketSocket socket3E = new MarketSocket(ETHdeltas.Skip(40).Take(20).ToList());
            socket3E.Connect();
            MarketSocket socket4E = new MarketSocket(ETHdeltas.Skip(60).Take(20).ToList());
            socket4E.Connect();

            var R = ETHdeltas.Skip(80).Take(20).ToList();

            if (R.Count > 0)
            {
                MarketSocket socket5E = new MarketSocket(R);
                socket5E.Connect();
            }


            Console.ReadLine();


        }




    }
    

}
