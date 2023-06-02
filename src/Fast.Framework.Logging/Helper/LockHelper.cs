using System.Threading;

internal static class LockHelper
{

    /// <summary>
    /// 互斥锁
    /// </summary>
    public static Mutex mutex = new Mutex();
}