using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using Stef.RedisInfrastructure;

namespace Stef.RedisTaskQueue
{
    public class TaskQueueWorker : IDisposable
    {
        private readonly ISubscriber _Subscriber;
        private readonly ITaskQueueJobHandler _Handler;
        private readonly string _WorkerId;
        private readonly string _GroupName;
        private readonly string _InfoName;
        private readonly string _WorkingQueueName;
        private readonly Action<TaskQueueWorker> _OnDispose;

        internal TaskQueueWorker(
            ISubscriber subscriber, 
            ITaskQueueJobHandler handler, 
            string workerId, 
            string groupName,
            Action<TaskQueueWorker> onDispose)
        {
            _Subscriber = subscriber;
            _Handler = handler;
            _WorkerId = workerId;
            _GroupName = groupName;
            _OnDispose = onDispose;

            _InfoName = TaskQueueManager.Current.GetFullInfoName(groupName);
            _WorkingQueueName = TaskQueueManager.Current.GetFullWorkingName(groupName, workerId);
        }

        public Task InitializationTask { get; private set; }
        public bool IsWorking { get; private set; }

        internal void Start()
        {
            InitializationTask = StartAsync();
        }
        private async Task StartAsync()
        {
            await RestoreHangingJobsAsync();
            SubscribeToNewJobs();
            await CheckWorkAsync();
        }

        private async Task RestoreHangingJobsAsync()
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var workQueueList = database.ListRange(_WorkingQueueName, start: 0);
            foreach (var workQueue in workQueueList)
            {
                 await HandleJobAsync(TaskQueueConstants.RESTORE_QUEUE_NAME, workQueue);
            }
        }

        private void SubscribeToNewJobs()
        {
            _Subscriber.Subscribe(
                TaskQueueManager.Current.GetFullChannelName(_GroupName),
                CheckWork);
        }

        private void CheckWork(RedisChannel channel, RedisValue value)
        {
            if (value != TaskQueueConstants.NEW_JOB)
                return;

            _ = CheckWorkAsync();
        }
        private async Task CheckWorkAsync()
        {
            lock (this)
            {
                if (IsWorking)
                    return;

                IsWorking = true;
            }

            try
            {
                await SpreadJobsFairUseAsync();
            }
            finally
            {
                lock (this)
                {
                    IsWorking = false;
                }
            }
        }

        private async Task SpreadJobsFairUseAsync()
        {
            while (true)
            {
                var taskQueueCountList = TaskQueueManager.Current.GetQueues(_GroupName);

                if (!taskQueueCountList.Any())
                    break;

                foreach (var taskQueue in taskQueueCountList.ToList())
                {
                    var value = GetNextJob(taskQueue);
                    if (value.IsNull)
                        continue;

                    await HandleJobAsync(taskQueue.FullQueueName, value);
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

            var condition = Condition.ListLengthGreaterThan(taskQueue.FullQueueName, 0);
            trans.AddCondition(condition);

            var value = trans.ListRightPopLeftPushAsync(
                taskQueue.FullQueueName,
                _WorkingQueueName);

            trans.HashDecrementAsync(
                _InfoName,
                taskQueue.FullQueueName, 
                1);

            trans.Execute();
            return value.Result;
        }

        private async Task HandleJobAsync(string queueName, string value)
        {
            var name = TaskQueueManager.Current.GetQueueName(queueName);
            var taskQueueJob = new TaskQueueJob(_WorkerId, name.GroupName, name.QueueName, value);

            try
            {
                var result = await _Handler.HandleJobAsync(taskQueueJob);

                if (result)
                {
                    var database = RedisManager
                       .Current
                       .GetConnection()
                       .GetDatabase();

                    database.ListRemove(
                        _WorkingQueueName, 
                        value);
                }
                else
                {
                    TaskQueueManager.Current.AddJob(
                        _GroupName,
                        name.QueueName, 
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
            _OnDispose(this);

            if (_Subscriber != null)
            {
                _Subscriber.Unsubscribe(
                    TaskQueueManager.Current.GetFullChannelName(_GroupName), 
                    CheckWork);
            }
        }
    }
}
