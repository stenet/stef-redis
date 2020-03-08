using System;
using System.Linq;
using System.Threading.Tasks;
using Stef.RedisTaskQueue;

namespace Stef.RedisTest
{
    public class TestTaskQueueHandler : ITaskQueueJobHandler
    {
        private readonly Func<TaskQueueJob, Task<bool>> _HandleJob;
        private readonly Action<TaskQueueJob, Exception> _OnException;

        public TestTaskQueueHandler(Func<TaskQueueJob, Task<bool>> handleJob, Action<TaskQueueJob, Exception> onException)
        {
            _HandleJob = handleJob;
            _OnException = onException;
        }

        public Task<bool> HandleJobAsync(TaskQueueJob taskQueueJob)
        {
            return _HandleJob(taskQueueJob);
        }

        public void OnException(TaskQueueJob taskQueueJob, Exception exception)
        {
            _OnException(taskQueueJob, exception);
        }
    }
}
