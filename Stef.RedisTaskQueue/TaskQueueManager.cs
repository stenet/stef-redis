using System;
using System.Linq;
using StackExchange.Redis;
using Stef.RedisInfrastructure;

namespace Stef.RedisTaskQueue
{
    public class TaskQueueManager
    {
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

        public void AddJob(string taskQueueName, string jobInfo)
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            taskQueueName = GetLongTaskQueueName(taskQueueName);

            var trans = database.CreateTransaction();

            trans.HashIncrementAsync(TaskQueueConstants.INFO_NAME, taskQueueName, 1);
            trans.ListLeftPushAsync(taskQueueName, jobInfo);

            trans.Execute();

            _Subscriber.Value.Publish(
                TaskQueueConstants.CHANNEL, 
                TaskQueueConstants.NEW_JOB, 
                CommandFlags.FireAndForget);
        }

        public void CleanUp()
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var hashes = database.HashGetAll(TaskQueueConstants.INFO_NAME);
            foreach (var hash in hashes)
            {
                var taskQueueName = (string)hash.Name;

                var trans = database.CreateTransaction();

                trans.AddCondition(Condition.HashLengthEqual(taskQueueName, 0));
                trans.HashDeleteAsync(TaskQueueConstants.INFO_NAME, taskQueueName);
                trans.KeyDeleteAsync(taskQueueName);

                trans.Execute();
            }
        }

        public void StartWorker(ITaskQueueJobHandler handler, string id)
        {
            if (_Worker != null)
                throw new InvalidOperationException("Worker already started");

            _Worker = new TaskQueueWorker(_Subscriber.Value, handler, id);
        }
        public void StopWorker()
        {
            if (_Worker == null)
                return;

            _Worker.Dispose();
            _Worker = null;
        }

        internal string GetLongTaskQueueName(string taskQueueName)
        {
            if (taskQueueName.StartsWith(TaskQueueConstants.PREFIX))
                return taskQueueName;
            else
                return string.Concat(TaskQueueConstants.PREFIX, taskQueueName);
        }
        internal string GetShortTaskQueueName(string taskQueueName)
        {
            if (taskQueueName.StartsWith(TaskQueueConstants.PREFIX))
                return taskQueueName.Substring(TaskQueueConstants.PREFIX.Length);
            else
                return taskQueueName;
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
