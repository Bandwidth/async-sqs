package com.bandwidth.sqs.queue.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.bandwidth.sqs.queue.buffer.task.Task;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.reactivex.subjects.CompletableSubject;

public class KeyedTaskBufferTest {
    private static final int MAX_BUFFER_SIZE = 3;
    private static final int BUFFER_SIZE_2 = 2;
    private static final Duration MAX_WAIT_MILLIS_INFINITE = Duration.ofMillis(Integer.MAX_VALUE);
    private static final Duration MAX_WAIT_MILLIS_100 = Duration.ofMillis(100);
    private static final Duration MAX_WAIT_MILLIS_0 = Duration.ofMillis(0);
    private static final String KEY_A = "a";
    private static final String KEY_B = "b";

    private int count = 0;

    private final Task<String, Integer> task = (key, map) -> map.values().forEach(value -> count += value);



    private final KeyedTaskBuffer<String, Integer> taskBuffer = new KeyedTaskBuffer<>(MAX_BUFFER_SIZE,
            MAX_WAIT_MILLIS_100, task
    );

    private final ScheduledExecutorService schedulerMock = mock(ScheduledExecutorService.class);
    private final ExecutorService executorMock = mock(ExecutorService.class);

    private final ArgumentCaptor<Runnable> scheduledTaskCaptor = ArgumentCaptor.forClass(Runnable.class);
    private final ArgumentCaptor<Runnable> executedTaskCaptor = ArgumentCaptor.forClass(Runnable.class);

    @Test
    public void testBufferFull() {
        KeyedTaskBuffer<String, Integer> taskBuffer = new KeyedTaskBuffer<>(schedulerMock, executorMock,
                MAX_BUFFER_SIZE, MAX_WAIT_MILLIS_INFINITE, task);

        taskBuffer.addData(KEY_A, 1); //creates new 'A' buffer, not full yet
        taskBuffer.addData(KEY_A, 2); //2nd element of 'A' buffer, not full yet
        taskBuffer.addData(KEY_B, 4); //creates new 'B' buffer, not full yet
        taskBuffer.addData(KEY_A, 8); //fills 'A' buffer, A task runner will run
        taskBuffer.addData(KEY_A, 16); //start of next 'A' buffer, not full yet

        verify(executorMock).submit(executedTaskCaptor.capture());
        executedTaskCaptor.getValue().run();

        assertThat(count).isEqualTo(1 + 2 + 8);
    }

    @Test
    public void testBufferNotFull() {
        taskBuffer.addData(KEY_A, 1);
        taskBuffer.addData(KEY_A, 2);
        taskBuffer.addData(KEY_B, 4);
        assertThat(count).isEqualTo(0);
    }

    @Test
    public void testBufferTimeout() {
        KeyedTaskBuffer<String, Integer> taskBuffer = new KeyedTaskBuffer<>(schedulerMock, executorMock,
                MAX_BUFFER_SIZE, MAX_WAIT_MILLIS_0, task);

        taskBuffer.addData(KEY_A, 1);
        taskBuffer.addData(KEY_A, 2);

        verify(schedulerMock).schedule(scheduledTaskCaptor.capture(), anyLong(), eq(TimeUnit.MILLISECONDS));
        scheduledTaskCaptor.getValue().run();

        verify(executorMock).submit(executedTaskCaptor.capture());
        executedTaskCaptor.getValue().run();

        assertThat(count).isEqualTo(1 + 2);
    }

    @Test
    public void testBufferTimeoutAfterEmptied() {
        KeyedTaskBuffer<String, Integer> taskBuffer = new KeyedTaskBuffer<>(schedulerMock, executorMock,
                MAX_BUFFER_SIZE, MAX_WAIT_MILLIS_0, task);
        taskBuffer.addData(KEY_A, 1);
        taskBuffer.addData(KEY_A, 2);
        taskBuffer.addData(KEY_A, 3);

        verify(schedulerMock).schedule(scheduledTaskCaptor.capture(), anyLong(), eq(TimeUnit.MILLISECONDS));
        scheduledTaskCaptor.getValue().run();

        verify(executorMock).submit(executedTaskCaptor.capture());
        executedTaskCaptor.getValue().run();

        assertThat(count).isEqualTo(1 + 2 + 3);
    }

    @Test
    public void shouldContinueRunningAfterExceptionInTask(){
        AtomicBoolean firstRun = new AtomicBoolean(true);
        CompletableSubject completedSecondRun = CompletableSubject.create();
        KeyedTaskBuffer<String, Integer> taskBuffer = new KeyedTaskBuffer<>(BUFFER_SIZE_2, MAX_WAIT_MILLIS_100,
                (key, batch) -> {
                    if(firstRun.getAndSet(false)){
                        throw new RuntimeException("Test exception");
                    }else{
                        completedSecondRun.onComplete();
                    }
                });
        taskBuffer.addData(KEY_A, 1);
        taskBuffer.addData(KEY_B, 1);

        //unit test can only finish if this is completed
        completedSecondRun.timeout(1000, TimeUnit.MILLISECONDS).blockingAwait();
    }
}

