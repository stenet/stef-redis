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

        private readonly string _Key;

        static DistributedLock()
        {
            OwnerIdentifier = Guid.NewGuid().ToString();

            DefaultExpiry = TimeSpan.FromSeconds(DEFAULT_EXPIRY_S);
            DefaultMaxWait = TimeSpan.Zero;
        }
        private DistributedLock(string key, bool hasLock, string lockOwner)
        {
            _Key = key;

            HasLock = hasLock;
            CurrentLockOwner = lockOwner;
        }

        public static string OwnerIdentifier { get; set; }

        public static TimeSpan DefaultExpiry { get; private set; }
        public static TimeSpan DefaultMaxWait { get; private set; }

        public bool HasLock { get; private set; }
        public string CurrentLockOwner { get; private set; }

        public void ExtendLock(TimeSpan expiry)
        {
            if (!HasLock)
                throw new LockTimeoutException();

            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var result = database.LockExtend(_Key, OwnerIdentifier, expiry);
            if (!result)
                throw new LockTimeoutException();
        }

        public static string GetCurrentLockOwner(string key)
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            return database.LockQuery(key);
        }

        public static DistributedLock GetLock(string key)
        {
            return GetLockAsync(key)
                .Result;
        }
        public static DistributedLock GetLock(string key, TimeSpan expiry)
        {
            return GetLockAsync(key, expiry)
                .Result;
        }
        public static DistributedLock GetLock(string key, TimeSpan expiry, TimeSpan maxWait)
        {
            return GetLockAsync(key, expiry, maxWait)
                .Result;
        }

        public static async Task<DistributedLock> GetLockAsync(string key)
        {
            return await GetLockAsync(key, DefaultExpiry, DefaultMaxWait);
        }
        public static async Task<DistributedLock> GetLockAsync(string key, TimeSpan expiry)
        {
            return await GetLockAsync(key, expiry, DefaultMaxWait);
        }
        public static async Task<DistributedLock> GetLockAsync(string key, TimeSpan expiry, TimeSpan maxWait)
        {
            var distributedLock = await TryGetLockAsync(key, expiry, maxWait);
            if (!distributedLock.HasLock)
                throw new LockTimeoutException();

            return distributedLock;
        }

        public static void ReleaseLock(string key)
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var lockOwner = GetCurrentLockOwner(key);
            if (string.IsNullOrEmpty(lockOwner))
                return;

            database.LockRelease(key, lockOwner);
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
            return await TryGetLockAsync(key, DefaultExpiry, DefaultMaxWait);
        }
        public static async Task<DistributedLock> TryGetLockAsync(string key, TimeSpan expiry)
        {
            return await TryGetLockAsync(key, expiry, DefaultMaxWait);
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
                var result = database.LockTake(key, OwnerIdentifier, expiry);

                if (result)
                    break;

                if (maxWait != TimeSpan.Zero)
                {
                    var waitHit = start.Add(maxWait) < DateTime.Now;
                    if (waitHit)
                        return new DistributedLock(key, false, GetCurrentLockOwner(key));
                }

                if (random == null)
                    random = new Random();

                await Task.Delay(random.Next(MIN_RETRY_MS, MAX_RETRY_MS));
            }

            return new DistributedLock(key, true, OwnerIdentifier);
        }
        
        private void ReleaseMyLock()
        {
            if (HasLock)
            {
                HasLock = false;

                var database = RedisManager
                   .Current
                   .GetConnection()
                   .GetDatabase();

                database.LockRelease(_Key, OwnerIdentifier);
            }
        }

        public void Dispose()
        {
            ReleaseMyLock();
        }
    }
}
