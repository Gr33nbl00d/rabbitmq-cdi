/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015, 2019 Patrick Reinhart
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package net.reini.rabbitmq.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultPublishRetryHandlerTest {

  @Mock
  private PublishFunction publishFunctionMock;
  @Mock
  private ErrorHandler<Object> errorHandlerMock;

  private DefaultPublishRetryHandler<Object> defaultPublishRetryHandler;

  @BeforeEach
  void setUp() {
    defaultPublishRetryHandler = new DefaultPublishRetryHandler<>(3, 4000);
  }

  @Test
  void testRetryUnrecoverable() throws TimeoutException, EncodeException, IOException, PublishException {
    final IOException runtimeException = new IOException("some text");
    doThrow(runtimeException).when(publishFunctionMock).publish();
    when(errorHandlerMock.isRetryable(runtimeException)).thenReturn(false);
    final PublishException publishException = Assertions.assertThrows(PublishException.class, () -> {
      defaultPublishRetryHandler.execute(publishFunctionMock, errorHandlerMock);
    });
    assertEquals("Non retryable error occurred", publishException.getMessage());
    assertEquals(runtimeException, publishException.getCause());
    verify(errorHandlerMock).isRetryable(runtimeException);
  }

  @Test
  public void testSleepBeforeRetry_InterruptedException() throws InterruptedException {
    call_SleepBeforeRetry_InAnotherThread_AndInterrupt();
  }

  private void call_SleepBeforeRetry_InAnotherThread_AndInterrupt() throws InterruptedException {
    final boolean[] interrupted = {false};
    Thread sleeper = new Thread("sleeper") {
      @Override
      public void run() {
        defaultPublishRetryHandler.sleepBeforeRetry();
        interrupted[0] = true;
      }
    };
    sleeper.start();
    Thread.sleep(500);
    sleeper.interrupt();
    sleeper.join(500);
    assertTrue(interrupted[0]);

  }
}