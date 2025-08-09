import TaskQueue, { Task, TaskId, TaskStatus } from '../src';
import { describe, expect, it, jest } from '@jest/globals';
import { delay } from './utils';

const BASE_TIME_FACTOR = 100;

describe('TaskQueue General Operations', () => {
  it('creates a queue with default concurrency', () => {
    const queue = new TaskQueue();
    expect(queue.getConcurrency()).toBe(1);

    expect(() => new TaskQueue({ concurrency: -1 })).toThrow(
      'Concurrency must be at least 1',
    );
  });

  it('subscribes to task status changes', async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const statusChanges: Array<{ taskId: TaskId; status: TaskStatus }> = [];

    queue.subscribeTaskStatusChange((status, task) => {
      statusChanges.push({ taskId: task.taskId, status });
    });

    await queue.addTask(async () => {
      await delay(1 * BASE_TIME_FACTOR);
      return 'done';
    }, 'task1');

    expect(statusChanges).toEqual([
      { taskId: 'task1', status: 'running' },
      { taskId: 'task1', status: 'success' },
    ]);

    await queue.addTask(async () => {
      await delay(1 * BASE_TIME_FACTOR);
      return 'done';
    }, 'task2');

    expect(statusChanges).toEqual([
      { taskId: 'task1', status: 'running' },
      { taskId: 'task1', status: 'success' },
      { taskId: 'task2', status: 'running' },
      { taskId: 'task2', status: 'success' },
    ]);
  });

  it('handles task status subscription and unsubscription', () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const statusChanges: Array<{ taskId: TaskId; status: string }> = [];

    // Test subscription with handler
    const handler = (status: string, task: Task) => {
      statusChanges.push({ taskId: task.taskId, status });
    };

    // Test unsubscribe function returned by subscribe
    const unsubscribe = queue.subscribeTaskStatusChange(handler);
    expect(typeof unsubscribe).toBe('function');

    // Test direct unsubscribe method
    queue.unsubscribeTaskStatusChange(handler);

    // Test unsubscribe via returned function
    const handler2 = jest.fn();
    const unsubscribe2 = queue.subscribeTaskStatusChange(handler2);
    unsubscribe2();

    // Verify handlers are properly unsubscribed
    queue.addTask(async () => 'test', 'test1');
    expect(handler).not.toHaveBeenCalled;
    expect(handler2).not.toHaveBeenCalled;
  });

  it('keeps existing listeners when unsubscribing non-existent handler', async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const handler1 = jest.fn();
    const handler2 = jest.fn();

    queue.subscribeTaskStatusChange(handler1);
    queue.subscribeTaskStatusChange(handler2);

    const nonListener = jest.fn();
    queue.unsubscribeTaskStatusChange(nonListener);

    await queue.addTask(async () => 'test', 'task1');

    expect(handler1).toHaveBeenCalled();
    expect(handler2).toHaveBeenCalled();
    expect(nonListener).not.toHaveBeenCalled();
  });

  it('retrieves task information', async () => {
    const queue = new TaskQueue({
      concurrency: 1,
      memorizeTasks: true,
      returnError: false,
    });

    const taskPromise = queue.addTask(async () => {
      await delay(1 * BASE_TIME_FACTOR);
      return 'result';
    }, 'task1');

    expect(queue.getTaskDetails('task1')).toMatchObject({
      taskId: 'task1',
      status: 'idle',
      priority: 'normal',
      queueId: 1,
    });

    await taskPromise;

    expect(queue.getTaskDetails('task1')).toMatchObject({
      taskId: 'task1',
      status: 'success',
      priority: 'normal',
      queueId: 1,
      result: 'result',
    });
  });

  it('controls queue execution with start/stop', async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const results: string[] = [];

    const promise1 = queue.addTask(async () => {
      results.push('task1');
      return 'task1';
    }, 'task1');

    queue.stop();
    expect(queue.isManuallyStopped()).toBe(true);

    await promise1;
    expect(results).toHaveLength(1);
    expect(results).toEqual(['task1']);

    const promise2 = queue.addTask(async () => {
      results.push('task2');
      return 'task2';
    }, 'task2');

    queue.start();
    expect(queue.isManuallyStopped()).toBe(false);

    await promise2;
    expect(results).toHaveLength(2);
    expect(results).toEqual(['task1', 'task2']);
  });

  it('retries failed tasks after queue stop', async () => {
    const queue = new TaskQueue({
      concurrency: 1,
      memorizeTasks: true,
      returnError: false,
    });

    let attempts = 0;
    const statusChanges: Array<{ taskId: TaskId; status: TaskStatus }> = [];

    queue.subscribeTaskStatusChange((status, task) => {
      statusChanges.push({ taskId: task.taskId, status });
    });

    const taskPromise = queue.addTask(async () => {
      attempts++;
      if (attempts === 1) {
        throw new Error('First attempt failure');
      }
      return 'success';
    }, 'retry-task');

    // First attempt should fail and stop the queue
    await expect(taskPromise).rejects.toThrow('First attempt failure');

    expect(statusChanges).toEqual([
      { taskId: 'retry-task', status: 'running' },
      { taskId: 'retry-task', status: 'error' },
    ]);

    expect(queue.getTaskDetails('retry-task')).toMatchObject({
      taskId: 'retry-task',
      status: 'error',
      error: new Error('First attempt failure'),
    });

    // Retry the failed task
    queue.retry();

    // Wait for the task to be retried
    await delay(1 * BASE_TIME_FACTOR);

    // Wait for retry to complete
    const result = queue.getTaskDetails('retry-task').result;

    expect(result).toBe('success');
    expect(attempts).toBe(2);

    expect(statusChanges).toEqual([
      { taskId: 'retry-task', status: 'running' },
      { taskId: 'retry-task', status: 'error' },
      { taskId: 'retry-task', status: 'running' },
      { taskId: 'retry-task', status: 'success' },
    ]);

    expect(queue.getTaskDetails('retry-task')).toMatchObject({
      taskId: 'retry-task',
      status: 'success',
      result: 'success',
    });
  });

  it('handles task cancellation', async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const results: string[] = [];

    queue.stop();

    const task1 = queue.addTask(async () => {
      results.push('task1');
      return 'task1';
    }, 'task1');

    const task2 = queue.addTask(async () => {
      results.push('task2');
      return 'task2';
    }, 'task2');

    queue.start();

    queue.removeWaitedTask('task2');

    await delay(1 * BASE_TIME_FACTOR);
    expect(results).toEqual(['task1']);
  });

  it('handles multiple task additions with addTasks', async () => {
    const queue = new TaskQueue({ concurrency: 2 });
    const results: number[] = [];

    queue.stop();

    const promise = queue.addTasks([
      {
        callback: async () => {
          results.push(1);
          return 'task1';
        },
        taskId: 'task1',
      },
      {
        callback: async () => {
          results.push(2);
          return 'task2';
        },
      },
      {
        callback: async () => {
          results.push(3);
          return 'task3';
        },
        taskId: 'task3',
        priority: 'important',
      },
      {
        callback: async () => {
          results.push(4);
          return 'task4';
        },
        priority: 'important',
      },
    ]);

    queue.start();

    const tasks = await promise;

    expect(results).toEqual([3, 4, 1, 2]); // Important task should complete first
    expect(tasks).toEqual(['task1', 'task2', 'task3', 'task4']);
  });

  it('manages task details with memorizeTasks enabled', async () => {
    const queue = new TaskQueue({ concurrency: 1, memorizeTasks: true });

    await queue.addTask(async () => 'task1', 'task1');
    await queue
      .addTask(async () => {
        throw new Error('Failed');
      }, 'task2')
      .catch(() => {});
    await queue.addTask(async () => 'task3', 'task3');

    // Test getAllTasksDetails
    const allTasks = queue.getAllTasksDetails();
    expect(allTasks).toHaveLength(3);

    // Test getAllTasksDetails with status filter
    const successTasks = queue.getAllTasksDetails('success');
    expect(successTasks).toHaveLength(2);

    const errorTasks = queue.getAllTasksDetails('error');
    expect(errorTasks).toHaveLength(1);

    // Test getAllTasksDetails with multiple status filter
    const mixedTasks = queue.getAllTasksDetails(['success', 'error']);
    expect(mixedTasks).toHaveLength(3);

    // Test clearTaskDetails
    queue.clearTaskDetails('task1');
    expect(queue.getTaskDetails('task1')).toBeUndefined();
    expect(queue.getAllTasksDetails()).toHaveLength(2);

    // Test clearAllTasksDetails
    queue.clearAllTasksDetails();
    expect(queue.getAllTasksDetails()).toHaveLength(0);
  });

  it('manages waiting tasks', async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const results: string[] = [];

    queue.stop();

    // Add multiple tasks while queue is stopped
    const task1 = queue.addTask(async () => {
      results.push('task1');
      return 'task1';
    }, 'task1');

    const task2 = queue.addTask(async () => {
      results.push('task2');
      return 'task2';
    }, 'task2');

    const task3 = queue.addTask(async () => {
      results.push('task3');
      return 'task3';
    }, 'task3');

    // Remove one waiting task
    queue.removeWaitedTask('task2');

    // Start queue and check execution
    queue.start();

    await Promise.all([task1, task3]);

    expect(results).toEqual(['task1', 'task3']);

    queue.stop();

    // Add more tasks and test clearWaitedTasks
    const task4 = queue.addTask(async () => {
      results.push('task4');
      return 'task4';
    }, 'task4');

    queue.clearWaitedTasks();
    queue.start();

    expect(results).toEqual(['task1', 'task3']);
  });

  it('manages failed retryable tasks', async () => {
    const queue = new TaskQueue({
      concurrency: 1,
      returnError: false,
      memorizeTasks: true,
    });

    // Add tasks that will fail
    const task1 = queue
      .addTask(async () => {
        throw new Error('Failed task 1');
      }, 'task1')
      .catch(() => {});

    const task2 = queue
      .addTask(async () => {
        throw new Error('Failed task 2');
      }, 'task2')
      .catch(() => {});

    await delay(1 * BASE_TIME_FACTOR);

    // Verify task details
    let failedTasks = queue.getAllTasksDetails('error');
    expect(failedTasks.map((task) => task.taskId)).toEqual(['task1']);

    // Remove one failed retryable task
    queue.removeFailedRetryableTask('task1');

    // Retry the failed task. Now, task 2 should be executed
    queue.retry();

    await delay(1 * BASE_TIME_FACTOR);

    // Verify task details
    failedTasks = queue.getAllTasksDetails('error');
    expect(failedTasks.map((task) => task.taskId)).toEqual(['task1', 'task2']);
  });

  it('throws error when accessing task details with memorizeTasks disabled', () => {
    const queue = new TaskQueue({ concurrency: 1, memorizeTasks: false });

    expect(() => queue.getTaskDetails('task1')).toThrow(
      'Memorizing task details is not enabled',
    );
    expect(() => queue.getAllTasksDetails()).toThrow(
      'Memorizing task details is not enabled',
    );
    expect(() => queue.clearTaskDetails('task1')).toThrow(
      'Memorizing task details is not enabled',
    );
    expect(() => queue.clearAllTasksDetails()).toThrow(
      'Memorizing task details is not enabled',
    );
  });

  it('handles task creation with different input types', async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const results: string[] = [];

    // Test with no taskId
    await queue.addTask(async () => {
      results.push('no-id');
      return 'no-id';
    });

    // Test with only status update handler
    await queue.addTask(
      async () => {
        results.push('with-handler');
        return 'with-handler';
      },
      (status, task) => {
        results.push(`${task.taskId}-${status}`);
      },
    );

    // Test with both taskId and status update handler
    await queue.addTask(
      async () => {
        results.push('both');
        return 'both';
      },
      'task-id',
      (status, task) => {
        results.push(`${task.taskId}-${status}`);
      },
    );

    expect(results).toContain('no-id');
    expect(results).toContain('with-handler');
    expect(results).toContain('both');
  });

  it('handles prioritized tasks creation with different input types', async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const results: string[] = [];

    // Test with no taskId
    await queue.addPrioritizedTask(async () => {
      results.push('no-id');
      return 'no-id';
    });

    // Test with only status update handler
    await queue.addPrioritizedTask(
      async () => {
        results.push('with-handler');
        return 'with-handler';
      },
      (status, task) => {
        results.push(`${task.taskId}-${status}`);
      },
    );

    // Test with both taskId and status update handler
    await queue.addPrioritizedTask(
      async () => {
        results.push('both');
        return 'both';
      },
      'task-id',
      (status, task) => {
        results.push(`${task.taskId}-${status}`);
      },
    );

    expect(results).toContain('no-id');
    expect(results).toContain('with-handler');
    expect(results).toContain('both');
  });

  it('handles task removal with different input types', async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const results: Array<string> = [];

    queue.stop();

    // Add some tasks
    const task1 = queue.addTask(async () => {
      results.push('task1');
      return 'task1';
    }, 'task1');
    const task2 = queue.addTask(async () => {
      results.push('task2');
      return 'task2';
    }, 'task2');

    // Test removing by task ID
    queue.removeWaitedTask('task1');

    // Test removing by task object
    const taskObject = {
      taskId: 'task2',
      callback: async () => 'task2',
      status: 'waiting' as const,
      priority: 'normal' as const,
    } as unknown as Task;
    queue.removeWaitedTask(taskObject);

    queue.start();

    await delay(1 * BASE_TIME_FACTOR);

    expect(results).toEqual([]);
  });

  it('handles failed retryable task removal with different input types', async () => {
    const queue = new TaskQueue({
      concurrency: 1,
      returnError: false,
      memorizeTasks: true,
    });

    // Add a failing task
    await queue
      .addTask(async () => {
        throw new Error('Failed');
      }, 'task1')
      .catch(() => {});

    // Test removing by task ID
    queue.removeFailedRetryableTask('task1');

    // Test removing by task object
    const taskObject = {
      taskId: 'task1',
      callback: async () => {
        throw new Error('Failed');
      },
      status: 'error' as const,
      priority: 'normal' as const,
    } as unknown as Task;
    queue.removeFailedRetryableTask(taskObject);

    // Verify task was removed
    expect(queue.getAllTasksDetails('error')).toHaveLength(1);
  });

  it('handles edge cases in task management', async () => {
    const queue = new TaskQueue({ concurrency: 1 });

    // Test adding tasks with undefined/null handlers
    await queue.addTask(async () => 'test', 'test1', undefined);
    await queue.addTask(async () => 'test', undefined, undefined);

    // Test adding prioritized tasks with undefined/null handlers
    await queue.addPrioritizedTask(async () => 'test', 'test2', undefined);
    await queue.addPrioritizedTask(async () => 'test', undefined, undefined);

    // Test removing non-existent tasks
    queue.removeWaitedTask('non-existent');
    queue.removeFailedRetryableTask('non-existent');

    // Test clearing empty queues
    queue.clearWaitedTasks();
    queue.clearFailedRetryableTasks();

    // All operations should complete without errors
    expect(true).toBe(true);
  });

  it('handles logger operations', async () => {
    const queue = new TaskQueue({
      concurrency: 1,
      logger: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
      },
      verbose: true,
    });

    queue.addTask(async () => {});

    expect(queue['logger'].info).toHaveBeenCalled();
  });

  it('should throw error when trying to run an already triggered task', async () => {
    const taskQueue = new TaskQueue();
    const task = {
      taskId: 'test-task',
      callback: async () => {
        // Simulate some async work
        await new Promise((resolve) => setTimeout(resolve, 10));
        return 'result';
      },
      status: 'running' as const, // Set initial status to running
    };

    try {
      await taskQueue['_runTask'](
        task as Task<string>,
        () => {},
        () => {},
      );
    } catch (error: any) {
      expect(error.message).toBe('Task test-task is already triggered');
    }
  });

  it('should return error when setting returnError to true', async () => {
    const queue = new TaskQueue({ returnError: true });
    const result = await queue.addTask(async () => {
      throw new Error('Test error');
    }, 'test1');
    expect(result).toBeInstanceOf(Error);
    expect((result as Error).message).toBe('Test error');
  });

  it('handles logger operations with different message formats', async () => {
    const mockLogger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };

    const queue = new TaskQueue({
      concurrency: 1,
      logger: mockLogger,
      verbose: true,
    });

    // Test logging with taskId
    queue['_log']({ level: 'info', taskId: 'test-task' }, 'Test message', {
      data: 'test',
    });
    expect(mockLogger.info).toHaveBeenCalledWith(
      'Task test-task |',
      'Test message',
      { data: 'test' },
    );

    // Test logging without taskId
    queue['_log']({ level: 'warn' }, 'Warning message', { warning: 'test' });
    expect(mockLogger.warn).toHaveBeenCalledWith('Warning message', {
      warning: 'test',
    });

    // Test that logging doesn't happen when verbose is false
    const nonVerboseQueue = new TaskQueue({
      concurrency: 1,
      logger: mockLogger,
      verbose: false,
    });

    nonVerboseQueue['_log']({ level: 'error' }, 'Error message');
    expect(mockLogger.error).not.toHaveBeenCalled();
  });
});
