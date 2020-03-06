using Stef.RedisInfrastructure;
using System;
using System.Threading;

namespace Stef.RedisDistributedLock
{
    public class DistributedLock
    {
        private const string TOKEN = "TOKEN";

        private readonly string _Key;
        private readonly Action _Action;
        private readonly int _ExpireSeconds;
        private readonly int _WaitSeconds;
        private readonly int _StealSeconds;

        public DistributedLock(string key, Action action, int expireSeconds = 60, int waitSeconds = -1, int stealSeconds = -1)
        {
            _Key = key;
            _Action = action;
            _ExpireSeconds = expireSeconds;
            _WaitSeconds = waitSeconds;
            _StealSeconds = stealSeconds;

            TryGetLock();
        }

        private void TryGetLock()
        {
            var redis = RedisManager
                .Current
                .GetConnection();

            var database = redis.GetDatabase();

            var start = DateTime.Now;
            Random random = null;
            while (true)
            {
                var result = database.LockTake(
                    _Key,
                    TOKEN,
                    TimeSpan.FromSeconds(_ExpireSeconds));

                if (result)
                    break;

                if (_WaitSeconds > 0)
                {
                    var seconds = (DateTime.Now - start).TotalSeconds;
                    if (seconds >= _WaitSeconds)
                        throw new InvalidOperationException("No lock received");
                }

                if (_StealSeconds > 0)
                {
                    var seconds = (DateTime.Now - start).TotalSeconds;
                    if (seconds >= _StealSeconds)
                        break;
                }

                if (random == null)
                    random = new Random();

                Thread.Sleep(random.Next(10, 500));
            }

            try
            {
                _Action();
            }
            finally
            {
                database.LockRelease(_Key, TOKEN);
            }
        }
    }
}
