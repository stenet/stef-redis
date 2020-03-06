using System;
using System.Linq;

namespace Stef.RedisTaskQueue
{
    internal static class TaskQueueConstants
    {
        internal const string PREFIX_NORMAL_PRIORITY = "TASK_QUEUE_NORMAL_PRIORITY";
        internal const string PREFIX_HIGH_PRIORITY = "TASK_QUEUE_HIGH_PRIORITY";
        internal const string PREFIX_WORKING = "TASK_QUEUE_WORKING_";

        internal const string RESTORE_QUEUE_NAME = "RESTORE";

        internal const string INFO_NAME = "TASK_QUEUE_INFO";

        internal const string CHANNEL = "TASK_QUEUE";
        internal const string NEW_JOB = "TASK_QUEUE_NEW_JOB";
    }
}
