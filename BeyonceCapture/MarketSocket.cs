using System;
using System.Collections.Generic;
using System.Text;
using System.Net.WebSockets;
using PureWebSockets;

namespace BeyonceCapture
{
    public class MarketSocket
    {
        private PureWebSocket _ws;
        public readonly string _marketDelta;

        public readonly List<string> _marketDeltas;

        public MarketSocket(string delta)
        {
            _marketDelta = delta;
            _ws = new PureWebSocket($"wss://stream.binance.com:9443/ws/{delta.ToLower()}@depth", new ReconnectStrategy(10000, 60000));
            _ws.OnStateChanged += Ws_OnStateChanged;
            _ws.OnMessage += Ws_OnMessage;
            _ws.OnClosed += Ws_OnClosed;
            _ws.OnSendFailed += _ws_OnSendFailed;
        }

        public MarketSocket(List<string> deltas)
        {
            _marketDeltas = deltas;
            var uri = $"wss://stream.binance.com:9443/stream?streams=";
            foreach (var delta in deltas)
                uri += $"{delta.ToLower()}@depth/";

            _ws = new PureWebSocket(uri, new ReconnectStrategy(10000, 60000));
            _ws.OnStateChanged += Ws_OnStateChanged;
            _ws.OnMessage += Ws_OnMessage;
            _ws.OnClosed += Ws_OnClosed;
            _ws.OnSendFailed += _ws_OnSendFailed;
            _ws.Connect();
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
            Console.WriteLine(message);
            //Console.ForegroundColor = ConsoleColor.Green;
            //Console.WriteLine($"{DateTime.Now} New message: {message}");
            //Console.ResetColor();
            //Console.WriteLine("");
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
