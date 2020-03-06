using System;
using System.Linq;
using StackExchange.Redis;
using Stef.RedisInfrastructure;

namespace Stef.RedisTaskQueue
{
    public class TaskQueueManager
    {
        private const string TASK_QUEUE_PREFIX = "TASK_QUEUE_";

        internal const string TASK_QUEUE_COUNT_NAME = "TASK_QUEUE_COUNT";
        internal const string TASK_QUEUE_WORKING_NAME = "TASK_QUEUE_WORKING";

        internal const string TASK_QUEUE_CHANNEL = "JOB_QUEUE";
        internal const string TASK_QUEUE_NEW_JOB = "TASK_QUEUE_NEW_ITEMS";

        private static Lazy<TaskQueueManager> _Current = new Lazy<TaskQueueManager>(() => new TaskQueueManager());

        private TaskQueueWorker _Worker;
        private Lazy<ISubscriber> _Subscriber;

        private TaskQueueManager()
        {
            _Subscriber = new Lazy<ISubscriber>(CreateSubscriber);
        }

        public static TaskQueueManager Current
        {
            get
            {
                return _Current.Value;
            }
        }

        public void AddJob(string taskQueue, string jobInfo)
        {
            var redis = RedisManager
                .Current
                .GetConnection();

            var db = redis.GetDatabase();

            var taskQueueName = taskQueue.StartsWith(TASK_QUEUE_PREFIX)
                ? taskQueue
                : string.Concat(TASK_QUEUE_PREFIX, taskQueue);

            var trans = db.CreateTransaction();

            trans.HashIncrementAsync(TASK_QUEUE_COUNT_NAME, taskQueueName, 1);
            trans.ListLeftPushAsync(taskQueueName, jobInfo);

            trans.Execute();

            _Subscriber
                .Value
                .Publish(TASK_QUEUE_CHANNEL, TASK_QUEUE_NEW_JOB, CommandFlags.FireAndForget);
        }

        public void CleanUp()
        {
            var redis = RedisManager
                .Current
                .GetConnection();

            var db = redis.GetDatabase();

            var hashes = db.HashGetAll(TASK_QUEUE_COUNT_NAME);
            foreach (var hash in hashes)
            {
                var taskQueueName = (string)hash.Name;

                var trans = db.CreateTransaction();

                trans.AddCondition(Condition.HashLengthEqual(taskQueueName, 0));
                trans.HashDeleteAsync(TASK_QUEUE_COUNT_NAME, taskQueueName);
                trans.KeyDeleteAsync(taskQueueName);

                trans.Execute();
            }
        }

        public void StartWorker(ITaskQueueJobHandler handler)
        {
            if (_Worker != null)
                throw new InvalidOperationException("Worker already started");

            _Worker = new TaskQueueWorker(_Subscriber.Value, handler);
        }
        public void StopWorker()
        {
            if (_Worker == null)
                return;

            _Worker.Dispose();
            _Worker = null;
        }

        private ISubscriber CreateSubscriber()
        {
            var redis = RedisManager
                .Current
                .GetConnection();

            return redis.GetSubscriber();
        }
    }
}
