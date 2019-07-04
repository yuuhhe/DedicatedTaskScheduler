// Copyright 2019 yuuhhe
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Yuuhhe.DedicatedTaskScheduler
{
    /// <summary>
    /// Provides dedicated thread(not threadpool) to process task.
    /// </summary>
    public class DedicatedTaskScheduler : TaskScheduler
    {
        const string SingleThreadName = "SingleDedicatedTaskScheduler";
        const string MultiThreadName = "MultiDedicatedTaskScheduler";
        private Thread thread;
        private Channel<Task> channel;
        private ChannelWriter<Task> writer;

        /// <summary>
        /// Indicates the maximum concurrency level this 
        /// <see cref="TaskScheduler"/> is able to support.
        /// </summary>
        public override int MaximumConcurrencyLevel { get; }

        /// <summary>
        /// Create a TaskScheduler that use only one thread to process task.
        /// </summary>
        /// <param name="threadName">name of thread</param>
        /// <returns>a DedicatedTaskScheduler instance</returns>
        public static TaskScheduler SingleThread(string threadName = SingleThreadName)
        {
            var taskScheduler = new DedicatedTaskScheduler(threadName);
            return taskScheduler;
        }
        /// <summary>
        /// Create a TaskScheduler that use <paramref name="concurrentLevel"/> threads to process task concurrently.
        /// </summary>
        /// <param name="concurrentLevel">concurrent level</param>
        /// <param name="threadName">name of thread</param>
        /// <returns>a DedicatedTaskScheduler instance</returns>
        /// <exception cref="T:System.ArgumentOutOfRangeException">The <paramref name="concurrentLevel"/> argument is less than 1.</exception>
        public static TaskScheduler MultiThread(int concurrentLevel = 1, string threadName = MultiThreadName)
        {
            if (concurrentLevel < 1) throw new ArgumentOutOfRangeException(nameof(concurrentLevel));
            var taskScheduler = new DedicatedTaskScheduler(threadName, concurrentLevel);
            return taskScheduler;
        }

        private DedicatedTaskScheduler(string threadName)
        {
            MaximumConcurrencyLevel = 1;
            channel = Channel.CreateUnbounded<Task>(new UnboundedChannelOptions() { SingleReader = true });
            writer = channel;

            thread = new Thread(Loop);
            thread.IsBackground = true;
            thread.Name = string.IsNullOrEmpty(threadName) ? SingleThreadName : threadName;
            thread.Start((ChannelReader<Task>)channel);
        }

        private DedicatedTaskScheduler(string threadName, int concurrentLevel)
        {
            MaximumConcurrencyLevel = concurrentLevel;
            channel = Channel.CreateUnbounded<Task>();
            writer = channel;

            for (int i = 0; i < concurrentLevel; i++)
            {
                thread = new Thread(Loop);
                thread.IsBackground = true;
                thread.Name = $"{(string.IsNullOrEmpty(threadName) ? MultiThreadName : threadName)}#{i + 1}";
                thread.Start((ChannelReader<Task>)channel);
            }
        }

        private void Loop(object obj)
        {
            var reader = (ChannelReader<Task>)obj;
            while (GetResult(reader.WaitToReadAsync()))
            {
                while (reader.TryRead(out var task))
                {
                    TryExecuteTask(task);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool GetResult(ValueTask<bool> valueTask)
        {
            if (valueTask.IsCompletedSuccessfully)
            {
                return valueTask.Result;
            }

            return valueTask.AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Generates an enumerable of <see cref="T:System.Threading.Tasks.Task">Task</see> instances
        /// currently queued to the scheduler waiting to be executed.
        /// </summary>
        /// <returns>An enumerable that allows traversal of tasks currently queued to this scheduler.
        /// </returns>
        protected override IEnumerable<Task> GetScheduledTasks() => Enumerable.Empty<Task>();


        /// <summary>
        /// Queues a <see cref="T:System.Threading.Tasks.Task">Task</see> to the scheduler.
        /// </summary>
        /// <param name="task">The <see cref="T:System.Threading.Tasks.Task">Task</see> to be queued.</param>
        protected override void QueueTask(Task task) => writer.TryWrite(task);

        /// <summary>
        /// Determines whether the provided <see cref="T:System.Threading.Tasks.Task">Task</see>
        /// can be executed synchronously in this call, and if it can, executes it.
        /// </summary>
        /// <param name="task">The <see cref="T:System.Threading.Tasks.Task">Task</see> to be executed.</param>
        /// <param name="taskWasPreviouslyQueued">A Boolean denoting whether or not task has previously been
        /// queued. If this parameter is True, then the task may have been previously queued (scheduled); if
        /// False, then the task is known not to have been queued, and this call is being made in order to
        /// execute the task inline without queueing it.</param>
        /// <returns>A Boolean value indicating whether the task was executed inline.</returns>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

        /// <summary>
        /// Attempts to dequeue a <see cref="T:System.Threading.Tasks.Task">Task</see> that was previously queued to
        /// this scheduler.
        /// </summary>
        /// <param name="task">The <see cref="T:System.Threading.Tasks.Task">Task</see> to be dequeued.</param>
        /// <returns>A Boolean denoting whether the <paramref name="task"/> argument was successfully dequeued.</returns>
        protected override bool TryDequeue(Task task) => false;
    }
}
