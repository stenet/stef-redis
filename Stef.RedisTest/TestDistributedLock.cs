using Microsoft.VisualStudio.TestTools.UnitTesting;
using Stef.RedisDistributedLock;
using Stef.RedisInfrastructure;
using System;

namespace Stef.RedisTest
{
    [TestClass]
    public class TestDistributedLock
    {
        [TestInitialize]
        public void Initialize()
        {
        }

        [TestMethod]
        public void TestGetLock()
        {
            var key = Guid.NewGuid().ToString();

            using (var l = DistributedLock.TryGetLock(key))
            {
                Assert.IsTrue(l.HasLock, "should have lock");
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
                Assert.IsTrue(l.HasLock, "should have lock");
            }
        }
        [TestMethod]
        public void TestLockOwner()
        {
            var key = Guid.NewGuid().ToString();

            using (var l = DistributedLock.TryGetLock(key))
            {
                Assert.AreEqual(l.CurrentLockOwner, DistributedLock.OwnerIdentifier, $"should has lock owner ${DistributedLock.OwnerIdentifier}");
            }
        }
        [TestMethod]
        public void TestLockOwner2()
        {
            var key = Guid.NewGuid().ToString();

            using (var l = DistributedLock.TryGetLock(key))
            {
                var lockOwner = DistributedLock.GetLockOwner(key);
                Assert.AreEqual(lockOwner, DistributedLock.OwnerIdentifier, $"should has lock owner ${DistributedLock.OwnerIdentifier}");
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
                    Assert.IsFalse(l2.HasLock, "shouldn't have lock");
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
                }, "should throw exception");
            }
        }
        [TestMethod]
        public void TestStealLock()
        {
            var key = Guid.NewGuid().ToString();

            using (var l1 = DistributedLock.TryGetLock(key))
            {
                DistributedLock.ReleaseLock(key);

                using (var l2 = DistributedLock.GetLock(key, DistributedLock.DefaultExpiry, TimeSpan.FromSeconds(1)))
                {
                    Assert.IsTrue(l2.HasLock, "should have stolen lock");
                }
            }
        }
    }
}
