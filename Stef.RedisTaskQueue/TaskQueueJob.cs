using System;

namespace Stef.RedisTaskQueue
{
    public class TaskQueueJob
    {
        public TaskQueueJob(string jobInfo)
        {
            JobInfo = jobInfo;
        }

        public string JobInfo { get; private set; }
    }
}
