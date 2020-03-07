using StackExchange.Redis;
using System;

namespace Stef.RedisInfrastructure
{
    public class RedisManager
    {
        private static Lazy<RedisManager> _Current = new Lazy<RedisManager>(() => new RedisManager());

        private Lazy<ConnectionMultiplexer> _Connection;

        private RedisManager()
        {
            _Connection = new Lazy<ConnectionMultiplexer>(CreateConnection);
        }

        public static RedisManager Current
        {
            get
            {
                return _Current.Value;
            }
        }

        public string ConnectionString { get; set; }

        public ConnectionMultiplexer GetConnection()
        {
            return _Connection.Value;
        }
        private ConnectionMultiplexer CreateConnection()
        {
            if (string.IsNullOrEmpty(ConnectionString))
                throw new ArgumentException("No ConnectionString defined");

            return ConnectionMultiplexer.Connect(ConnectionString);
        }
    }
}
