using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Net.WebSockets;
using PureWebSockets;

namespace BeyonceCapture
{
    public class MarketSocket
    {
        private PureWebSocket _ws;
        public readonly List<string> _marketDeltas;
        private ConcurrentDictionary<string, int> UpdateNonces;
                

        public MarketSocket(List<string> deltas)
        {
            _marketDeltas = deltas;
            UpdateNonces = new ConcurrentDictionary<string, int>();

            var uri = $"wss://stream.binance.com:9443/stream?streams=";
            foreach (var delta in deltas)
            {
                uri += $"{delta.ToLower()}@depth/{delta.ToLower()}@trade/";
            }

            _ws = new PureWebSocket(uri, new ReconnectStrategy(10000, 60000));
            _ws.OnStateChanged += Ws_OnStateChanged;
            _ws.OnMessage += Ws_OnMessage;
            _ws.OnClosed += Ws_OnClosed;
            _ws.OnSendFailed += _ws_OnSendFailed;
        }

        public bool Connect()
        {
            return _ws.Connect();

        }


        private void _ws_OnSendFailed(string data, Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{DateTime.Now} Send Failed: {ex.Message}");
            Console.ResetColor();
            Console.WriteLine("");
        }

        private void OnTick(object state)
        {
            _ws.Send(DateTime.Now.Ticks.ToString());
        }

        private void Ws_OnClosed(WebSocketCloseStatus reason)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{DateTime.Now} Connection Closed: {reason}");
            Console.ResetColor();
            Console.WriteLine("");
            Console.ReadLine();
        }

        private void Ws_OnMessage(string message)
        {
            BeyonceDATA.UpdateQueue.Enqueue(message);

            //var subject = (Utf8Json.JsonSerializer.Deserialize<streamSubjectLine>(message)).stream.Split('@')[1];
            //Console.WriteLine(subject);
            
        }

        private void Ws_OnStateChanged(WebSocketState newState, WebSocketState prevState)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"{DateTime.Now} Status changed from {prevState} to {newState}");
            Console.ResetColor();
            Console.WriteLine("");
        }
    }
}
