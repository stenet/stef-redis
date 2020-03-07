using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using Stef.RedisInfrastructure;

namespace Stef.RedisTaskQueue
{
    internal class TaskQueueWorker : IDisposable
    {
        private readonly ISubscriber _Subscriber;
        private readonly ITaskQueueJobHandler _Handler;
        private readonly string _TaskQueueWorkingQueueName;

        private bool _IsWorking;

        public TaskQueueWorker(ISubscriber subscriber, ITaskQueueJobHandler handler, string id)
        {
            _Subscriber = subscriber;
            _Handler = handler;
            _TaskQueueWorkingQueueName = string.Concat(TaskQueueConstants.PREFIX_WORKING, id);

            RestoreHangingJobs();
            SubscribeToNewJobs();
            CheckWork();
        }

        private void RestoreHangingJobs()
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            database.ListRange(_TaskQueueWorkingQueueName, start: 0)
                .ToList()
                .ForEach(c => HandleJob(TaskQueueConstants.RESTORE_QUEUE_NAME, c));
        }

        private void SubscribeToNewJobs()
        {
            _Subscriber.Subscribe(TaskQueueConstants.CHANNEL, CheckWork);
        }

        private void CheckWork(RedisChannel channel, RedisValue value)
        {
            if (value != TaskQueueConstants.NEW_JOB)
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
            while (true)
            {
                var taskQueueCountList = GetTaskQueueCountList();

                if (!taskQueueCountList.Any())
                    break;

                foreach (var taskQueue in taskQueueCountList.ToList())
                {
                    var value = GetNextJob(taskQueue);
                    if (value.IsNull)
                        continue;

                    HandleJob(taskQueue.TaskQueueName, value);
                }
            }
        }
        private RedisValue GetNextJob(TaskQueueCountItem taskQueue)
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var trans = database.CreateTransaction();

            var condition = Condition.ListLengthGreaterThan(taskQueue.TaskQueueName, 0);
            trans.AddCondition(condition);

            var value = trans.ListRightPopLeftPushAsync(
                taskQueue.TaskQueueName,
                _TaskQueueWorkingQueueName);

            trans.HashDecrementAsync(
                TaskQueueConstants.INFO_NAME,
                taskQueue.TaskQueueName, 
                1);

            trans.Execute();
            return value.Result;
        }
        private List<TaskQueueCountItem> GetTaskQueueCountList()
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var itemList = database
                .HashGetAll(TaskQueueConstants.INFO_NAME)
                .Select(c => new TaskQueueCountItem(c.Name, (long)c.Value))
                .Where(c => c.Count > 0)
                .ToList();

            var hasHighPriority = itemList
                .Any(c => c.TaskQueueName.StartsWith(TaskQueueConstants.PREFIX_HIGH_PRIORITY));

            if (hasHighPriority)
            {
                return itemList
                    .Where(c => c.TaskQueueName.StartsWith(TaskQueueConstants.PREFIX_HIGH_PRIORITY))
                    .ToList();
            }

            return itemList;
        }

        private void HandleJob(string taskQueueName, RedisValue value)
        {
            taskQueueName = TaskQueueManager.Current.GetShortTaskQueueName(taskQueueName);
            var taskQueueJob = new TaskQueueJob(taskQueueName, value);

            try
            {
                var result = _Handler.HandleJob(taskQueueJob);

                if (result)
                {
                    var database = RedisManager
                       .Current
                       .GetConnection()
                       .GetDatabase();

                    database.ListRemove(
                        _TaskQueueWorkingQueueName, 
                        value);
                }
                else
                {
                    TaskQueueManager.Current.AddJob(
                        taskQueueName, 
                        value);
                }
            }
            catch (Exception ex)
            {
                _Handler.OnException(taskQueueJob, ex);
            }
        }

        public void Dispose()
        {
            if (_Subscriber != null)
                _Subscriber.Unsubscribe(TaskQueueConstants.CHANNEL, CheckWork);
        }
    }
}
