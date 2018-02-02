using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;

namespace BeyonceCapture
{
    public static class BeyonceDATA
    {
        public static ConcurrentQueue<MarketDepthMsg> MarketDataQueue = new ConcurrentQueue<MarketDepthMsg>();






    }

    
}
