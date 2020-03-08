using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;
using Stef.RedisInfrastructure;

namespace Stef.RedisTaskQueue
{
    public class TaskQueueManager
    {
        private static Lazy<TaskQueueManager> _Current = new Lazy<TaskQueueManager>(() => new TaskQueueManager());

        private Lazy<ISubscriber> _Subscriber;
        private List<TaskQueueWorker> _WorkerList;

        private TaskQueueManager()
        {
            _WorkerList = new List<TaskQueueWorker>();
            _Subscriber = new Lazy<ISubscriber>(CreateSubscriber);
        }

        public static TaskQueueManager Current
        {
            get
            {
                return _Current.Value;
            }
        }

        public void AddJob(string groupName, string queueName, string jobInfo)
        {
            ValidateParameters(groupName, queueName, jobInfo);

            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var infoName = GetFullInfoName(groupName);
            var channelName = GetFullChannelName(groupName);
            queueName = GetFullTaskQueueName(groupName, queueName);

            var trans = database.CreateTransaction();

            trans.SetAddAsync(TaskQueueConstants.GROUP_NAME, groupName);
            trans.HashIncrementAsync(infoName, queueName, 1);
            trans.ListLeftPushAsync(queueName, jobInfo);

            trans.Execute();

            _Subscriber.Value.Publish(
                channelName,
                TaskQueueConstants.NEW_JOB,
                CommandFlags.FireAndForget);
        }
        private void ValidateParameters(string groupName, string queueName, string jobInfo)
        {
            if (string.IsNullOrEmpty(groupName))
                throw new ArgumentException($"{nameof(groupName)} is null or empty.", nameof(groupName));

            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentException($"{nameof(queueName)} is null or empty.", nameof(queueName));

            if (string.IsNullOrEmpty(jobInfo))
                throw new ArgumentException($"{nameof(jobInfo)} is null or empty.", nameof(jobInfo));

            ValidateName(groupName, nameof(groupName));
            ValidateName(queueName, nameof(queueName));
        }
        private void ValidateName(string name, string propertyName)
        {
            if (name.Contains(";"))
                throw new ArgumentException($"Semicolon not allowed in {propertyName}: {name}");
        }

        public void CleanUp(bool force = false)
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var groups = database.SetMembers(TaskQueueConstants.GROUP_NAME);
            foreach (var groupName in groups)
            {
                var infoName = GetFullInfoName(groupName);

                CleanUpHashAndQueue(database, infoName, force);
                CleanUpGroup(database, groupName, infoName, force);
            }
        }
        private void CleanUpHashAndQueue(IDatabase database, string infoName, bool force)
        {
            var hashes = database.HashGetAll(infoName);
            foreach (var hash in hashes)
            {
                var taskQueueName = (string)hash.Name;

                var trans = database.CreateTransaction();

                if (!force)
                    trans.AddCondition(Condition.HashLengthEqual(taskQueueName, 0));

                trans.HashDeleteAsync(infoName, taskQueueName);
                trans.KeyDeleteAsync(taskQueueName);

                trans.Execute();
            }
        }
        private void CleanUpGroup(IDatabase database, string groupName, string infoName, bool force)
        {
            var trans = database.CreateTransaction();

            if (!force)
                trans.AddCondition(Condition.HashLengthEqual(infoName, 0));

            trans.SetRemoveAsync(TaskQueueConstants.GROUP_NAME, groupName);

            trans.Execute();
        }

        public TaskQueueWorker CreateWorker(ITaskQueueJobHandler handler, string groupName, string workerId)
        {
            if (handler == null)
                throw new ArgumentNullException(nameof(handler), $"{nameof(handler)} is null.");

            if (string.IsNullOrEmpty(groupName))
                throw new ArgumentException($"{nameof(groupName)} is null or empty.", nameof(groupName));

            if (string.IsNullOrEmpty(workerId))
                throw new ArgumentException($"{nameof(workerId)} is null or empty.", nameof(workerId));

            ValidateName(groupName, "groupName");

            var worker = new TaskQueueWorker(
                _Subscriber.Value, 
                handler, 
                workerId, 
                groupName,
                (w) => _WorkerList.Remove(w));

            _WorkerList.Add(worker);
            worker.Start();

            return worker;
        }
        public void StopAllWorkers()
        {
            while (_WorkerList.Any())
            {
                var worker = _WorkerList.First();
                worker.Dispose();

                _WorkerList.Remove(worker);
            }
        }
        public IEnumerable<TaskQueueWorker> GetWorkers()
        {
            return _WorkerList.AsEnumerable();
        }

        public IEnumerable<string> GetGroups()
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            return database
                .SetMembers(TaskQueueConstants.GROUP_NAME)
                .Select(c => (string)c)
                .ToList();
        }
        public IEnumerable<TaskQueueCountItem> GetQueues(string groupName)
        {
            var database = RedisManager
                .Current
                .GetConnection()
                .GetDatabase();

            var infoName = GetFullInfoName(groupName);

            return database
                .HashGetAll(infoName)
                .Select(c => new TaskQueueCountItem(c.Name, GetQueueName(c.Name).QueueName, (long)c.Value))
                .Where(c => c.Count > 0)
                .ToList();
        }

        internal string GetFullTaskQueueName(string groupName, string queueName)
        {
            return string.Concat(
                TaskQueueConstants.QUEUE_PREFIX,
                ";",
                groupName,
                ";",
                queueName);
        }
        internal string GetFullInfoName(string groupName)
        {
            return string.Concat(
                TaskQueueConstants.INFO_NAME,
                ";",
                groupName);
        }
        internal string GetFullChannelName(string groupName)
        {
            return string.Concat(
                TaskQueueConstants.CHANNEL,
                ";",
                groupName);
        }
        internal string GetFullWorkingName(string groupName, string workerId)
        {
            return string.Concat(
                TaskQueueConstants.WORKING_PREFIX,
                ";",
                groupName,
                ";",
                workerId);
        }
        internal TaskQueueName GetQueueName(string queueName)
        {
            var tokens = queueName.Split(';');

            if (tokens.Length == 2)
                return new TaskQueueName(tokens[0], tokens[1]);
            else
                return new TaskQueueName(tokens[1], tokens[2]);
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
