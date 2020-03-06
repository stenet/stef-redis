using System;
using System.Collections.Generic;
using System.Text;

namespace Stef.RedisTaskQueue
{
    public interface ITaskQueueJobHandler
    {
        bool HandleJob(TaskQueueJob taskQueueJob);
        void OnException(TaskQueueJob taskQueueJob, Exception exception);
    }
}
