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
            var BTCpairs = GetPairingsByVol(infos, "BTC");
            var ETHpairs = GetPairingsByVol(infos, "ETH");
            //var BNBpairs = GetPairingsByVol(infos, "BNB");
            //var USDTpairs = GetPairingsByVol(infos, "USDT");
            
            BeyonceDATA.StartDataUpdates();
            MarketSocket socket = new MarketSocket(BTCpairs.Take(20).ToList());
            socket.Connect();
            MarketSocket socket2 = new MarketSocket(BTCpairs.Skip(20).Take(20).ToList());
            socket2.Connect();
            MarketSocket socket3 = new MarketSocket(BTCpairs.Skip(40).Take(20).ToList());
            socket3.Connect();
            MarketSocket socket4 = new MarketSocket(BTCpairs.Skip(60).Take(20).ToList());
            socket4.Connect();
            MarketSocket socket5 = new MarketSocket(BTCpairs.Skip(80).Take(20).ToList());
            socket5.Connect();


            MarketSocket socketE = new MarketSocket(ETHpairs.Take(20).ToList());
            socketE.Connect();
            MarketSocket socket2E = new MarketSocket(ETHpairs.Skip(20).Take(20).ToList());
            socket2E.Connect();
            MarketSocket socket3E = new MarketSocket(ETHpairs.Skip(40).Take(20).ToList());
            socket3E.Connect();
            MarketSocket socket4E = new MarketSocket(ETHpairs.Skip(60).Take(20).ToList());
            socket4E.Connect();

            var R = ETHpairs.Skip(80).Take(20).ToList();

            if (R.Count > 0)
            {
                MarketSocket socket5E = new MarketSocket(R);
                socket5E.Connect();
            }


            Console.ReadLine();


        }

        private static List<string> GetPairingsByVol(List<Info24h> infos, string quoteSymbol)
        {            
            return (from info in infos
                    where info.symbol.Substring(info.symbol.Length - quoteSymbol.Length, quoteSymbol.Length) == quoteSymbol
                    orderby info.quoteVolume descending
                    select info.symbol).ToList();
        }


    }
    

}
