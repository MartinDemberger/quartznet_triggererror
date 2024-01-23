using Quartz;

namespace SchedulerErrorTest
{
    [DisallowConcurrentExecution]
    public class ExecuterJob : IJob
    {
        private static readonly SemaphoreSlim _runningLock = new SemaphoreSlim(0);
        private static readonly SemaphoreSlim _waitingLock = new SemaphoreSlim(0);
        private static int _running = 0;

        public ExecuterJob()
        {
        }

        public async Task Execute(IJobExecutionContext context)
        {
            Interlocked.Increment(ref _running);
            try
            {
                _runningLock.Release(1);
                await _waitingLock.WaitAsync();
            }
            finally
            {
                Interlocked.Decrement(ref _running);
            }
        }

        public static async Task WaitTillRunning()
        {
            await _runningLock.WaitAsync();
        }

        public static void ContinueRunning()
        {
            _waitingLock.Release(1);
        }

        public static int Running => _running;
    }
}
