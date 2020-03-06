using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;
using Stef.RedisInfrastructure;

namespace Stef.RedisTaskQueue
{
    internal class TaskQueueWorker : IDisposable
    {
        private readonly ISubscriber _Subscriber;
        private readonly ITaskQueueJobHandler _Handler;

        private bool _IsWorking;

        public TaskQueueWorker(ISubscriber subscriber, ITaskQueueJobHandler handler)
        {
            _Subscriber = subscriber;
            _Handler = handler;

            SubscribeToNewJobs();
            CheckWork();
        }

        private void SubscribeToNewJobs()
        {
            _Subscriber.Subscribe(TaskQueueManager.TASK_QUEUE_CHANNEL, CheckWork);
        }

        private void CheckWork(RedisChannel channel, RedisValue value)
        {
            if (value != TaskQueueManager.TASK_QUEUE_NEW_JOB)
                return;

            CheckWork();
        }
        private void CheckWork()
        {
            lock (this)
            {
                if (_IsWorking)
                    return;

                _IsWorking = true;
            }

            try
            {
                SpreadJobsFairUse();
            }
            finally
            {
                lock (this)
                {
                    _IsWorking = false;
                }
            }
        }

        private void SpreadJobsFairUse()
        {
            var redis = RedisManager
                .Current
                .GetConnection();

            var db = redis.GetDatabase();

            while (true)
            {
                var taskQueueCountList = db
                    .HashGetAll(TaskQueueManager.TASK_QUEUE_COUNT_NAME)
                    .Select(c => new TaskQueueCountItem(c.Name, (long)c.Value))
                    .Where(c => c.Count > 0)
                    .ToList();

                if (!taskQueueCountList.Any())
                    break;

                foreach (var taskQueue in taskQueueCountList.ToList())
                {
                    var trans = db.CreateTransaction();
                    
                    trans.AddCondition(Condition.ListLengthGreaterThan(taskQueue.TaskQueueName, 0));
                    var value = db.ListRightPopLeftPushAsync(taskQueue.TaskQueueName, TaskQueueManager.TASK_QUEUE_WORKING_NAME);
                    db.HashDecrement(TaskQueueManager.TASK_QUEUE_COUNT_NAME, taskQueue.TaskQueueName, 1);

                    trans.Execute();

                    if (value.Result.IsNull)
                        continue;

                    HandleJob(db, taskQueue, value.Result);
                }
            }
        }

        private void HandleJob(IDatabase db, TaskQueueCountItem taskQueue, RedisValue value)
        {
            var taskQueueJob = new TaskQueueJob(value);

            try
            {
                var result = _Handler.HandleJob(taskQueueJob);

                if (result)
                    db.ListRemove(TaskQueueManager.TASK_QUEUE_WORKING_NAME, value);
                else
                    TaskQueueManager.Current.AddJob(taskQueue.TaskQueueName, value);
            }
            catch (Exception ex)
            {
                _Handler.OnException(taskQueueJob, ex);
            }
        }

        public void Dispose()
        {
            if (_Subscriber != null)
                _Subscriber.Unsubscribe(TaskQueueManager.TASK_QUEUE_CHANNEL, CheckWork);
        }
    }
}
