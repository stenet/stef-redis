using System;
using System.Diagnostics;
using System.Threading;
using Stef.RedisTaskQueue;

namespace Stef.RedisTestConsole
{
    public class TaskQueueJobHandler : ITaskQueueJobHandler
    {
        private static Random _Random = new Random();
        private static int _Count;

        public bool HandleJob(TaskQueueJob taskQueueJob)
        {
            //if (_Count == 5)
                //Debugger.Break();

            _Count++;

            var val = _Random.Next(100, 300);
            Thread.Sleep(val);

            Console.WriteLine($"{taskQueueJob.TaskQueueName}: {taskQueueJob.JobInfo}");
            return true;
        }

        public void OnException(TaskQueueJob taskQueueJob, Exception exception)
        {
            Console.WriteLine($"Exception: {exception.Message} on {taskQueueJob.JobInfo}");
        }
    }
}
