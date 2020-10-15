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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;

public class GenericPublisher<T> implements MessagePublisher<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericPublisher.class);

  private final DeclarerRepository declarerRepository;
  private final ConnectionRepository connectionRepository;

  public GenericPublisher(ConnectionRepository connectionRepository) {
    this.connectionRepository = connectionRepository;
    this.declarerRepository = new DeclarerRepository();
  }

  @Override
  public void publish(T event, PublisherConfiguration<T> publisherConfiguration)
      throws PublishException {
    final PublishRetryHandler<T> retryHandler = publisherConfiguration.getPublishRetryHandler();
    retryHandler.execute(() -> {
      try (Channel channel =
          connectionRepository.getConnection(publisherConfiguration.getConfig()).createChannel()) {
        if (publisherConfiguration.getPublisherConfirmConfiguration().usePublisherConfirms()) {
          channel.confirmSelect();
        }
        List<Declaration> declarations = publisherConfiguration.getDeclarations();
        declarerRepository.declare(channel, declarations);
        publisherConfiguration.publish(channel, event);
        if (publisherConfiguration.getPublisherConfirmConfiguration().usePublisherConfirms()) {
          waitForConfirms(channel, publisherConfiguration.getPublisherConfirmConfiguration());
        }
      } catch (NoConfirmationReceivedException e) {
        throw new PublishException(e);
      }
    }, publisherConfiguration.getErrorHandler());

  }

  private void waitForConfirms(Channel channel, PublishConfirmConfiguration publisherConfirmConfiguration) throws NoConfirmationReceivedException {
    try {
      if (publisherConfirmConfiguration.getTimeout() == 0) {
        channel.waitForConfirmsOrDie();
      } else {
        channel.waitForConfirmsOrDie(publisherConfirmConfiguration.getTimeout());
      }
    } catch (IOException | TimeoutException e) {
      throw new NoConfirmationReceivedException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new NoConfirmationReceivedException(e);
    }
  }

  @Override
  public void publishBatch(RabbitMqBatchEvent<T> batchEvent, PublisherConfiguration<T> publisherConfiguration)
      throws PublishException {
    final PublishRetryHandler<T> retryHandler = publisherConfiguration.getPublishRetryHandler();
    final List<T> listEventsToExecute = new ArrayList<>(batchEvent.getListEvents());
    retryHandler.execute(() -> {
      try (Channel channel =
          connectionRepository.getConnection(publisherConfiguration.getConfig()).createChannel()) {
        if (publisherConfiguration.getPublisherConfirmConfiguration().usePublisherConfirms()) {
          channel.confirmSelect();
        }
        List<Declaration> declarations = publisherConfiguration.getDeclarations();
        declarerRepository.declare(channel, declarations);
        try {
          for (T listEvent : new ArrayList<>(listEventsToExecute)) {
            publisherConfiguration.publish(channel, listEvent);
            listEventsToExecute.remove(listEvent);
          }
        } finally {
          if (publisherConfiguration.getPublisherConfirmConfiguration().usePublisherConfirms() && listEventsToExecute.size() < batchEvent.getListEvents().size()) {
            waitForConfirms(channel, publisherConfiguration.getPublisherConfirmConfiguration());
          }
        }
        return;
      } catch (NoConfirmationReceivedException e) {
        throw new PublishException(e);
      }
    }, publisherConfiguration.getErrorHandler());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
  }
}
