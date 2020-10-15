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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultPublishRetryHandler<T> implements PublishRetryHandler<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericPublisher.class);
  private static final int DEFAULT_RETRY_ATTEMPTS = 3;
  private static final int DEFAULT_RETRY_INTERVAL = 1000;

  private final int maxRetryAttempts;
  private final long retryInterval;

  public DefaultPublishRetryHandler() {
    this.maxRetryAttempts = DEFAULT_RETRY_ATTEMPTS;
    this.retryInterval = DEFAULT_RETRY_INTERVAL;
  }

  public DefaultPublishRetryHandler(int maxRetryAttempts, long retryInterval) {
    this.maxRetryAttempts = maxRetryAttempts;
    this.retryInterval = retryInterval;
  }

  @Override
  public void execute(PublishFunction publishFunction, ErrorHandler<T> errorHandler) throws PublishException {
    int attempt;
    for (attempt = 1; attempt <= maxRetryAttempts; attempt++) {
      try {
        publishFunction.publish();
        return;
      } catch (EncodeException e) {
        throw new PublishException("Unable to serialize event", e);
      } catch (IOException | TimeoutException e) {
        handleError(errorHandler, attempt, e);
      }
      sleepBeforeRetry();
      if (attempt > 1) {
        LOGGER.debug("Attempt {} to send message", Integer.valueOf(attempt));
      }
    }
  }

  private void handleError(ErrorHandler<T> errorHandler, int attempt, Exception e) throws PublishException {
    if (errorHandler.isRetryable(e) == false) {
      throw new PublishException("Non retryable error occurred", e);
    } else {
      if (attempt == maxRetryAttempts) {
        throw new PublishException("Unable to send message after " + attempt + " attempts", e);
      } else {
        LOGGER.warn("Error sending message to broker: {] - Retrying..." + e.getMessage());
        LOGGER.debug("Error sending message to broker: {] - Retrying..." + e.getMessage(), e);
      }
    }
  }

  void sleepBeforeRetry() {
    try {
      Thread.sleep(retryInterval);
    } catch (InterruptedException e) {
      LOGGER.warn("Sending message interrupted while waiting for retry attempt", e);
      Thread.currentThread().interrupt();
    }
  }

}
