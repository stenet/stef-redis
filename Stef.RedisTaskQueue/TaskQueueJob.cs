using System;

namespace Stef.RedisTaskQueue
{
    public class TaskQueueJob
    {
        public TaskQueueJob(string taskQueueName, string jobInfo)
        {
            TaskQueueName = taskQueueName;
            JobInfo = jobInfo;
        }

        public string TaskQueueName { get; private set; }
        public string JobInfo { get; private set; }
    }
}
