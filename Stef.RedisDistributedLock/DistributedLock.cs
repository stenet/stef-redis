using Stef.RedisInfrastructure;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Stef.RedisDistributedLock
{
    public class DistributedLock : IDisposable
    {
        private const int DEFAULT_EXPIRY_S = 60;
        private const int MIN_RETRY_MS = 5;
        private const int MAX_RETRY_MS = 500;

        private static string _Token;

        private readonly string _Key;
        private bool _HasLock;

        static DistributedLock()
        {
            _Token = Guid.NewGuid().ToString();
        }
        private DistributedLock(string key)
        {
            _Key = key;
            _HasLock = true;
        }

        public static DistributedLock TryGetLock(string key)
        {
            return TryGetLockAsync(key)
                .Result;
        }
        public static DistributedLock TryGetLock(string key, TimeSpan expiry)
        {
            return TryGetLockAsync(key, expiry)
                .Result;
        }
        public static DistributedLock TryGetLock(string key, TimeSpan expiry, TimeSpan maxWait)
        {
            return TryGetLockAsync(key, expiry, maxWait)
                .Result;
        }

        public static async Task<DistributedLock> TryGetLockAsync(string key)
        {
            return await TryGetLockAsync(key, TimeSpan.FromSeconds(DEFAULT_EXPIRY_S), TimeSpan.Zero);
        }
        public static async Task<DistributedLock> TryGetLockAsync(string key, TimeSpan expiry)
        {
            return await TryGetLockAsync(key, expiry, TimeSpan.Zero);
        }
        public static async Task<DistributedLock> TryGetLockAsync(string key, TimeSpan expiry, TimeSpan maxWait)
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();
            
            var start = DateTime.Now;
            Random random = null;
            while (true)
            {
                var result = database.LockTake(key, _Token, expiry);

                if (result)
                    break;

                if (maxWait != TimeSpan.Zero)
                {
                    var waitHit = start.Add(maxWait) < DateTime.Now;
                    if (waitHit)
                        throw new LockTimeoutException();
                }

                if (random == null)
                    random = new Random();

                await Task.Delay(random.Next(MIN_RETRY_MS, MAX_RETRY_MS));
            }

            return new DistributedLock(key);
        }

        public void ExtendLock(TimeSpan expiry)
        {
            if (!_HasLock)
                throw new LockTimeoutException();

            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var result = database.LockExtend(_Key, _Token, expiry);
            if (!result)
                throw new LockTimeoutException();
        }

        private void ReleaseLock()
        {
            if (_HasLock)
            {
                _HasLock = false;

                var database = RedisManager
                   .Current
                   .GetConnection()
                   .GetDatabase();

                database.LockRelease(_Key, _Token);
            }
        }

        public void Dispose()
        {
            ReleaseLock();
        }
    }
}
