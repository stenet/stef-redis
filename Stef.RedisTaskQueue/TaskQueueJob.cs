using System;

namespace Stef.RedisTaskQueue
{
    public class TaskQueueJob
    {
        public TaskQueueJob(string workerId, string groupName, string queueName, string jobInfo)
        {
            WorkerId = workerId;
            GroupName = groupName;
            QueueName = queueName;
            JobInfo = jobInfo;
        }

        public string WorkerId { get; private set; }
        public string GroupName { get; private set; }
        public string QueueName { get; private set; }
        public string JobInfo { get; private set; }
    }
}
