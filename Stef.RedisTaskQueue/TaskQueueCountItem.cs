using System;
using System.Linq;

namespace Stef.RedisTaskQueue
{
    internal class TaskQueueCountItem
    {
        public TaskQueueCountItem(string taskQueueName, long count)
        {
            TaskQueueName = taskQueueName;
            Count = count;
        }

        public string TaskQueueName { get; private set; }
        public long Count { get; private set; }
    }
}
