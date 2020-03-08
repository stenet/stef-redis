using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Stef.RedisTaskQueue
{
    public interface ITaskQueueJobHandler
    {
        Task<bool> HandleJobAsync(TaskQueueJob taskQueueJob);
        void OnException(TaskQueueJob taskQueueJob, Exception exception);
    }
}
