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

            //TestLocks();
            TestTasks(args.Any(c => c == "worker"));

            Console.ReadLine();
        }

        private static void TestLocks()
        {
            RunLockTask("1");
            RunLockTask("2");
            RunLockTask("3", true);
            RunLockTask("4");
        }
        private static void TestTasks(bool isWorker)
        {
            TaskQueueManager
                .Current
                .CleanUp();

            if (isWorker)
            {
                TaskQueueManager
                    .Current
                    .StartWorker(new TaskQueueJobHandler());
            }
            else
            {
                while (true)
                {
                    TaskQueueManager.Current.AddJob("TEST01", "A");
                    TaskQueueManager.Current.AddJob("TEST01", "A");
                    TaskQueueManager.Current.AddJob("TEST01", "A");
                    TaskQueueManager.Current.AddJob("TEST01", "A");
                    TaskQueueManager.Current.AddJob("TEST01", "A");
                    TaskQueueManager.Current.AddJob("TEST01", "A");
                    TaskQueueManager.Current.AddJob("TEST02", "B");
                    TaskQueueManager.Current.AddJob("TEST02", "B");
                    TaskQueueManager.Current.AddJob("TEST02", "B");
                    TaskQueueManager.Current.AddJob("TEST03", "C");

                    Console.ReadLine();
                }
            }
        }

        private static void RunLockTask(string taskName, bool throwException = false, int stealSeconds = -1)
        {
            var task = Task.Run(() =>
            {
                new DistributedLock(LOCK_NAME, () =>
                {
                    Console.WriteLine($"Start: {taskName}");
                    Thread.Sleep(1000);

                    if (throwException)
                        throw new InvalidOperationException("all went wrong");

                    Console.WriteLine($"Finish: {taskName}");
                }, stealSeconds: stealSeconds);
            });

            task.ContinueWith(t =>
            {
                Console.WriteLine($"Exception \"{t.Exception.Message}\" occured in task {taskName}");
            }, TaskContinuationOptions.OnlyOnFaulted);
        }
    }
}
