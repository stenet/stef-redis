using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Stef.RedisDistributedLock;
using Stef.RedisInfrastructure;

namespace Stef.RedisTest
{
    [TestClass]
    public class Initialize
    {
        [AssemblyInitialize]
        public static void AssemblyInitialize(TestContext context)
        {
            RedisManager.Current.ConnectionString = "localhost";
            RedisManager.Current.GetConnection();

            DistributedLock.OwnerIdentifier = "TEST";
        }
    }
}
