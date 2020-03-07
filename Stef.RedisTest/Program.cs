using Stef.RedisDistributedLock;
using Stef.RedisInfrastructure;
using Stef.RedisTaskQueue;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Stef.RedisTest
{
    class Program
    {
        private const string LOCK_NAME = "TEST";

        static void Main(string[] args)
        {
            RedisManager
                .Current
                .ConnectionString = "localhost";

            //Do this in first place, as otherwise results in a TimeOut with TestLocks
            var conn = RedisManager
                .Current
                .GetConnection();

            TestLocks();
            //TestTasks(args.Any(c => c == "worker"));

            Console.ReadLine();
        }

        private static void TestLocks()
        {
            var lockName = "TEST2";
            var primLock = DistributedLock.GetLock(lockName);
            
            DistributedLock.OwnerIdentifier = "STEFAN";
            DistributedLock.ReleaseLock(lockName);

            var secLock = DistributedLock.GetLock(lockName);

            return;

            for (int i = 0; i < 100; i++)
            {
                RunLockTask("1");
                RunLockTask("2");
                RunLockTask("3", true);
                RunLockTask("4");
            }
        }
        private static void RunLockTask(string taskName, bool throwException = false)
        {
            var task = Task.Run(async () =>
            {
                using (var l = await DistributedLock.GetLockAsync(LOCK_NAME))
                {
                    Console.WriteLine($"Start: {taskName}");
                    Thread.Sleep(1000);

                    Console.WriteLine($"Finish: {taskName}");
                }
            });

            task.ContinueWith(t =>
            {
                Console.WriteLine($"Exception \"{t.Exception.Message}\" occured in task {taskName}");
            }, TaskContinuationOptions.OnlyOnFaulted);
        }

        private static void TestTasks(bool isWorker)
        {
            TaskQueueManager
                .Current
                .CleanUp();

            TaskQueueManager
                .Current
                .StartWorker(new TaskQueueJobHandler(), "TEST");

            //return;

            for (int i = 0; i < 10; i++)
            {
                TaskQueueManager.Current.AddJob("TEST01", "1");
                TaskQueueManager.Current.AddJob("TEST01", "2");
                TaskQueueManager.Current.AddJob("TEST01", "3");
                TaskQueueManager.Current.AddJob("TEST01", "4");
                TaskQueueManager.Current.AddJob("TEST01", "5");
                TaskQueueManager.Current.AddJob("TEST01", "6");
                TaskQueueManager.Current.AddJob("TEST02", "7");
                TaskQueueManager.Current.AddJob("TEST02", "8");
                TaskQueueManager.Current.AddJob("TEST02", "9");
                TaskQueueManager.Current.AddJob("TEST03", "10");
            }

            TaskQueueManager.Current.AddJob("TEST01", "PRIO", isHighPriority: true);
        }
    }
}
