using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Stef.RedisTaskQueue;

namespace Stef.RedisTest
{
    [TestClass]
    public class TestTaskQueue
    {
        public const string DEFAULT_QUEUE = "default";
        public const string DEFAULT_WORKER_ID = "1";

        [TestInitialize]
        public void Initialize()
        {
        }
        [TestCleanup]
        public void Cleanup()
        {
            TaskQueueManager
                .Current
                .CleanUp(force: true);
        }

        [TestMethod]
        public void TestAddJob()
        {
            var group = Guid.NewGuid().ToString();

            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, group);

            var hasGroup = TaskQueueManager.Current.GetGroups()
                .Any(c => c == group);

            Assert.IsTrue(hasGroup);

            var queues = TaskQueueManager.Current.GetQueues(group);
            Assert.AreEqual(1, queues.Count());

            Assert.AreEqual(DEFAULT_QUEUE, queues.First().QueueName);
            Assert.AreEqual(1, queues.First().Count);
        }
        [TestMethod]
        public void TestCleanup()
        {
            var group = Guid.NewGuid().ToString();

            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, group);

            var groupCount = TaskQueueManager.Current.GetGroups().Count();
            Assert.AreEqual(1, groupCount);

            TaskQueueManager.Current.CleanUp();
            groupCount = TaskQueueManager.Current.GetGroups().Count();
            Assert.AreEqual(1, groupCount);
            
            TaskQueueManager.Current.CleanUp(force: true);
            groupCount = TaskQueueManager.Current.GetGroups().Count();
            Assert.AreEqual(0, groupCount);
        }
        [TestMethod]
        public async Task TestWorker()
        {
            var group = Guid.NewGuid().ToString();
            var jobInfo = group;

            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);
            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);
            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);

            var count = 0;
            var handler = new TestTaskQueueHandler((job) =>
            {
                Assert.AreEqual(DEFAULT_WORKER_ID, job.WorkerId);
                Assert.AreEqual(group, job.GroupName);
                Assert.AreEqual(DEFAULT_QUEUE, job.QueueName);
                Assert.AreEqual(jobInfo, job.JobInfo);
                count++;
                return Task.FromResult(true);
            }, (job, exception) =>
            {
                throw new ApplicationException("", exception);
            });

            var worker = TaskQueueManager.Current.CreateWorker(handler, group, DEFAULT_WORKER_ID);
            await worker.InitializationTask;

            Assert.AreEqual(3, count);
        }
        [TestMethod]
        public async Task TestWorkerException()
        {
            var group = Guid.NewGuid().ToString();
            var jobInfo = group;

            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);

            var count = 0;
            var handler = new TestTaskQueueHandler((job) =>
            {
                throw new InvalidOperationException();
            }, (job, exception) =>
            {
                count++;
            });

            var worker = TaskQueueManager.Current.CreateWorker(handler, group, DEFAULT_WORKER_ID);
            await worker.InitializationTask;

            Assert.AreEqual(1, count);
        }
        [TestMethod]
        public async Task TestWorkerSpreadFairUse()
        {
            var group = Guid.NewGuid().ToString();
            var jobInfo = group;

            var queue1 = "queue1";
            var queue2 = "queue2";
            var queue3 = "queue3";

            TaskQueueManager.Current.AddJob(group, queue1, jobInfo);
            TaskQueueManager.Current.AddJob(group, queue1, jobInfo);
            TaskQueueManager.Current.AddJob(group, queue2, jobInfo);
            TaskQueueManager.Current.AddJob(group, queue3, jobInfo);

            var count = 0;
            var handler = new TestTaskQueueHandler((job) =>
            {
                switch (count)
                {
                    case 0:
                        Assert.AreEqual(queue1, job.QueueName);
                        break;
                    case 1:
                        Assert.AreEqual(queue2, job.QueueName);
                        break;
                    case 2:
                        Assert.AreEqual(queue3, job.QueueName);
                        break;
                    default:
                        Assert.AreEqual(queue1, job.QueueName);
                        break;
                }

                count++;
                return Task.FromResult(true);
            }, (job, exception) =>
            {
                throw new ApplicationException("", exception);
            });

            var worker = TaskQueueManager.Current.CreateWorker(handler, group, DEFAULT_WORKER_ID);
            await worker.InitializationTask;  
        }
        [TestMethod]
        public async Task TestWorkerRequeue()
        {
            var group = Guid.NewGuid().ToString();
            var jobInfo = group;

            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);

            var count = 0;
            var handler = new TestTaskQueueHandler((job) =>
            {
                count++;

                return Task.FromResult(count != 1);
            }, (job, exception) =>
            {
                throw new ApplicationException("", exception);
            });

            var worker = TaskQueueManager.Current.CreateWorker(handler, group, DEFAULT_WORKER_ID);
            await worker.InitializationTask;

            Assert.AreEqual(2, count);
        }
        [TestMethod]
        public async Task TestWorkerSubscriber()
        {
            var group = Guid.NewGuid().ToString();
            var jobInfo = group;

            var count = 0;
            var handler = new TestTaskQueueHandler((job) =>
            {
                Assert.AreEqual(jobInfo, job.JobInfo);
                count++;
                return Task.FromResult(true);
            }, (job, exception) =>
            {
                throw new ApplicationException("", exception);
            });

            var worker = TaskQueueManager.Current.CreateWorker(handler, group, DEFAULT_WORKER_ID);
            await worker.InitializationTask;

            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);

            await Task.Delay(200);

            Assert.AreEqual(1, count);
        }
        [TestMethod]
        public async Task TestWorkerMultiple()
        {
            var group = Guid.NewGuid().ToString();
            var jobInfo = group;

            const string workerId1 = "worker1";
            const string workerId2 = "worker2";

            var countWorkerId1 = 0;
            var countWorkerId2 = 0;

            var handler = new TestTaskQueueHandler(async (job) =>
            {
                switch (job.WorkerId)
                {
                    case workerId1:
                        countWorkerId1++;
                        break;
                    case workerId2:
                        countWorkerId2++;
                        break;
                }

                await Task.Delay(100);
                return true;
            }, (job, exception) =>
            {
                throw new ApplicationException("", exception);
            });

            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);
            TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);

            var worker1 = TaskQueueManager.Current.CreateWorker(handler, group, workerId1);
            var worker2 = TaskQueueManager.Current.CreateWorker(handler, group, workerId2);

            await worker1.InitializationTask;
            await worker2.InitializationTask;

            await Task.Delay(400);

            Assert.AreEqual(1, countWorkerId1);
            Assert.AreEqual(1, countWorkerId2);
        }
        [TestMethod]
        public async Task TestWorkerDispose()
        {
            var group = Guid.NewGuid().ToString();

            var handler = new TestTaskQueueHandler((job) =>
            {
                return Task.FromResult(true);
            }, (job, exception) =>
            {
                throw new ApplicationException("", exception);
            });

            var worker = TaskQueueManager.Current.CreateWorker(handler, group, DEFAULT_WORKER_ID);
            
            var workers = TaskQueueManager.Current
                .GetWorkers()
                .Count(c => c == worker);

            Assert.AreEqual(1, workers);

            await worker.InitializationTask;
            worker.Dispose();

            workers = TaskQueueManager.Current
                .GetWorkers()
                .Count(c => c == worker);

            Assert.AreEqual(0, workers);
        }
        [TestMethod]
        public async Task TestWorkerDisposeAll()
        {
            var group = Guid.NewGuid().ToString();

            var handler = new TestTaskQueueHandler((job) =>
            {
                return Task.FromResult(true);
            }, (job, exception) =>
            {
                throw new ApplicationException("", exception);
            });

            var worker = TaskQueueManager.Current.CreateWorker(handler, group, DEFAULT_WORKER_ID);

            var workers = TaskQueueManager.Current
               .GetWorkers()
               .Count(c => c == worker);

            Assert.AreEqual(1, workers);

            await worker.InitializationTask;

            TaskQueueManager.Current.StopAllWorkers();

            workers = TaskQueueManager.Current
               .GetWorkers()
               .Count(c => c == worker);

            Assert.AreEqual(0, workers);
        }
        [TestMethod]
        public void TestValidateGroupName()
        {
            var group = "a:b";
            var jobInfo = group;

            Assert.ThrowsException<ArgumentException>(() =>
            {
                TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, jobInfo);
            });
            Assert.ThrowsException<ArgumentException>(() =>
            {
                TaskQueueManager.Current.AddJob(group, null, jobInfo);
            });
        }
        [TestMethod]
        public void TestValidateQueueName()
        {
            var group = Guid.NewGuid().ToString();
            var jobInfo = group;
            var queue = "a:b";

            Assert.ThrowsException<ArgumentException>(() =>
            {
                TaskQueueManager.Current.AddJob(group, queue, jobInfo);
            });
            Assert.ThrowsException<ArgumentException>(() =>
            {
                TaskQueueManager.Current.AddJob(group, null, jobInfo);
            });
        }
        [TestMethod]
        public void TestValidateJobInfo()
        {
            var group = Guid.NewGuid().ToString();

            Assert.ThrowsException<ArgumentException>(() =>
            {
                TaskQueueManager.Current.AddJob(group, DEFAULT_QUEUE, null);
            });
        }
    }
}
