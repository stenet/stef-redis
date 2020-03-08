using System;
using System.Linq;

namespace Stef.RedisTaskQueue
{
    internal static class TaskQueueConstants
    {
        internal const string QUEUE_PREFIX = "TQ";
        internal const string WORKING_PREFIX = "TQ";

        internal const string RESTORE_QUEUE_NAME = "RESTORE";

        internal const string GROUP_NAME = "TQGROUP";
        internal const string INFO_NAME = "TQINFO";

        internal const string CHANNEL = "TASK_QUEUE";
        internal const string NEW_JOB = "TASK_QUEUE_NEW_JOB";
    }
}
