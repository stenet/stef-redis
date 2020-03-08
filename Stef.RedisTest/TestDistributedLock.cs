using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Stef.RedisDistributedLock;

namespace Stef.RedisTest
{
    [TestClass]
    public class TestDistributedLock
    {
        [TestInitialize]
        public void Initialize()
        {
        }
        [TestCleanup]
        public void Cleanup()
        {
            DistributedLock.Owner = RedisTest.Initialize.DEFAULT_OWNER;
        }

        [TestMethod]
        public void TestGetLock()
        {
            var key = Guid.NewGuid().ToString();

            using (var l = DistributedLock.TryGetLock(key))
            {
                Assert.IsTrue(l.HasLock);
            }
        }
        [TestMethod]
        public void TestGetLockSequential()
        {
            var key = Guid.NewGuid().ToString();

            using (var l = DistributedLock.TryGetLock(key))
            {
            }

            using (var l = DistributedLock.TryGetLock(key))
            {
                Assert.IsTrue(l.HasLock);
            }
        }
        [TestMethod]
        public void TestLockOwner()
        {
            var key = Guid.NewGuid().ToString();

            using (var l = DistributedLock.TryGetLock(key))
            {
                Assert.AreEqual(DistributedLock.Owner, l.CurrentLockOwner);
            }
        }
        [TestMethod]
        public void TestLockOwner2()
        {
            var key = Guid.NewGuid().ToString();

            using (var l = DistributedLock.TryGetLock(key))
            {
                var lockOwner = DistributedLock.GetLockOwner(key);
                Assert.AreEqual(DistributedLock.Owner, lockOwner);
            }
        }
        [TestMethod]
        public void TestGetLockTwoTimesWithTry()
        {
            var key = Guid.NewGuid().ToString();

            using (var l1 = DistributedLock.TryGetLock(key))
            {
                using (var l2 = DistributedLock.TryGetLock(key, DistributedLock.DefaultExpiry, TimeSpan.FromSeconds(1)))
                {
                    Assert.IsFalse(l2.HasLock);
                }
            }
        }
        [TestMethod]
        public void TestGetLockTwoTimesWithException()
        {
            var key = Guid.NewGuid().ToString();

            using (var l1 = DistributedLock.TryGetLock(key))
            {
                Assert.ThrowsException<AggregateException>(() =>
                {
                    using (var l2 = DistributedLock.GetLock(key, DistributedLock.DefaultExpiry, TimeSpan.FromSeconds(1)))
                    {
                    }
                });
            }
        }
        [TestMethod]
        public void TestStealLock()
        {
            var key = Guid.NewGuid().ToString();

            using (var l1 = DistributedLock.TryGetLock(key))
            {
                var newOwner = "TEST2";

                try
                {
                    DistributedLock.Owner = newOwner;
                    DistributedLock.ReleaseLock(key);

                    using (var l2 = DistributedLock.GetLock(key, DistributedLock.DefaultExpiry, TimeSpan.FromSeconds(1)))
                    {
                        Assert.IsTrue(l2.HasLock);
                        Assert.AreEqual(newOwner, l2.CurrentLockOwner);
                    }
                }
                finally
                {
                }
            }
        }
        [TestMethod]
        public async Task TestExtend()
        {
            var key = Guid.NewGuid().ToString();

            using (var l1 = DistributedLock.TryGetLock(key, TimeSpan.FromMilliseconds(200)))
            {
                l1.ExtendLock();

                await Task.Delay(300);

                var lockOwner = DistributedLock.GetLockOwner(key);
                Assert.AreEqual(RedisTest.Initialize.DEFAULT_OWNER, lockOwner);
            }
        }
        [TestMethod]
        public async Task TestExtendFail()
        {
            var key = Guid.NewGuid().ToString();

            using (var l1 = DistributedLock.TryGetLock(key, TimeSpan.FromMilliseconds(200)))
            {
                await Task.Delay(300);

                Assert.ThrowsException<LockTimeoutException>(() =>
                {
                    l1.ExtendLock();
                });
            }
        }
    }
}
