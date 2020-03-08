using System;
using System.Linq;

namespace Stef.RedisTaskQueue
{
    internal class TaskQueueName
    {
        public TaskQueueName(string groupName, string queueName)
        {
            GroupName = groupName;
            QueueName = queueName;
        }

        public string GroupName { get; private set; }
        public string QueueName { get; private set; }
    }
}
