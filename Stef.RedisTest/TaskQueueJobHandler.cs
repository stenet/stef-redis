using System;
using System.Threading;
using Stef.RedisTaskQueue;

namespace Stef.RedisTest
{
    public class TaskQueueJobHandler : ITaskQueueJobHandler
    {
        private static Random _Random = new Random();

        public bool HandleJob(TaskQueueJob taskQueueJob)
        {
            //var val = _Random.Next(200, 700);
            //Thread.Sleep(val);

            Console.WriteLine($"Task: {taskQueueJob.JobInfo}");
            return true;
        }

        public void OnException(TaskQueueJob taskQueueJob, Exception exception)
        {
            Console.WriteLine($"Exception: {exception.Message} on {taskQueueJob.JobInfo}");
        }
    }
}
