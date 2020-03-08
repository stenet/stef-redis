using System;
using System.Linq;

namespace Stef.RedisTaskQueue
{
    public class TaskQueueCountItem
    {
        internal TaskQueueCountItem(string fullQueueName, string queueName, long count)
        {
            FullQueueName = fullQueueName;
            QueueName = queueName;
            Count = count;
        }

        public string QueueName { get; private set; }
        public long Count { get; private set; }

        internal string FullQueueName { get; set; }
    }
}
