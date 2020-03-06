using System;
using System.Linq;

namespace Stef.RedisTaskQueue
{
    internal static class TaskQueueConstants
    {
        internal const string PREFIX = "TASK_QUEUE_";
        internal const string WORKING_PREFIX = "TASK_QUEUE_WORKING_";

        internal const string RESTORE_QUEUE_NAME = "RESTORE";

        internal const string INFO_NAME = "TASK_QUEUE_INFO";

        internal const string CHANNEL = "TASK_QUEUE";
        internal const string NEW_JOB = "TASK_QUEUE_NEW_JOB";
    }
}
