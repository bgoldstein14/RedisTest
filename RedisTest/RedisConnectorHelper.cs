using System;
using StackExchange.Redis;

public class RedisConnectorHelper
{
    static RedisConnectorHelper()
    {
        //RedisConnectorHelper.lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect("s-qmwas01:6379"));
        RedisConnectorHelper.lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect("d-portsolapp01:6379"));
        //RedisConnectorHelper.lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect("dldenrdsapp01.lx.ts.jhg.global:6379,password=a2rLmGY&Wj$%W9u0"));
        //RedisConnectorHelper.lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect("dldenrdsapp01.lx.ts.jhg.global:6379"));
    }

    private static Lazy<ConnectionMultiplexer> lazyConnection;

    public static ConnectionMultiplexer Connection => lazyConnection.Value;
}
