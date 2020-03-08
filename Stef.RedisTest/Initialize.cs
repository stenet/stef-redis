using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Stef.RedisDistributedLock;
using Stef.RedisInfrastructure;
using Stef.RedisTaskQueue;

namespace Stef.RedisTest
{
    [TestClass]
    public class Initialize
    {
        public const string DEFAULT_OWNER = "TEST";
        public const string CONNECTION_STRING = "localhost";

        [AssemblyInitialize]
        public static void AssemblyInitialize(TestContext context)
        {
            RedisManager.Current.ConnectionString = CONNECTION_STRING;
            RedisManager.Current.GetConnection();

            TaskQueueManager.Current.CleanUp(force: true);

            DistributedLock.Owner = DEFAULT_OWNER;
        }
    }
}
