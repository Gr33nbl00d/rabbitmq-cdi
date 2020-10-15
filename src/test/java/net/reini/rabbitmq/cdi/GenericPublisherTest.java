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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

@ExtendWith(MockitoExtension.class)
public class GenericPublisherTest {
  @Mock
  private ConnectionConfig config;
  @Mock
  private ConnectionRepository connectionRepository;
  @Mock
  private Connection connection;
  @Mock
  private Channel channel;
  @Mock
  private Encoder<TestEvent> encoder;
  @Mock
  private ErrorHandler<TestEvent> errorHandler;

  private List<ExchangeDeclaration> declarations = new ArrayList<>();
  private GenericPublisher<TestEvent> publisher;
  private TestEvent event;
  private Function<TestEvent, String> routingKeyFunction;
  private PublishRetryHandler retryHandler;
  private PublishConfirmConfiguration publishConfirmConfiguration;

  @BeforeEach
  public void setUp() throws Exception {
    publisher = new GenericPublisher<TestEvent>(connectionRepository);
    this.retryHandler = new DefaultPublishRetryHandler(3, 0);
    event = new TestEvent();
    event.id = "theId";
    event.booleanValue = true;
    routingKeyFunction = e -> "routingKey";
    this.publishConfirmConfiguration = new PublishConfirmConfiguration(false);
  }

  @Test
  public void testPublish() throws Exception {
    Builder builder = new Builder();
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, publishConfirmConfiguration);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);

    publisher.publish(event, publisherConfiguration);

    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishBatchWithConfirms() throws Exception {
    Builder builder = new Builder();
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);

    List<TestEvent> eventList = Collections.singletonList(event);
    publisher.publishBatch(new RabbitMqBatchEvent<>(eventList, TestEvent.class), publisherConfiguration);

    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie();
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishBatchWithConfirmsAndTimeout() throws Exception {
    Builder builder = new Builder();
    final int confirmTimeout = 3000;
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true, confirmTimeout);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);

    List<TestEvent> eventList = Collections.singletonList(event);
    publisher.publishBatch(new RabbitMqBatchEvent<>(eventList, TestEvent.class), publisherConfiguration);

    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie(confirmTimeout);
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishBatchWithConfirms_NackError() throws Exception {
    Builder builder = new Builder();
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
    final IOException expectedCause = new IOException("nack");
    doThrow(expectedCause).when(channel).waitForConfirmsOrDie();

    List<TestEvent> eventList = Collections.singletonList(event);
    final PublishException publishException = assertThrows(PublishException.class, () -> {
      publisher.publishBatch(new RabbitMqBatchEvent<>(eventList, TestEvent.class), publisherConfiguration);
    });
    assertEquals("java.io.IOException: nack", publishException.getCause().getMessage());
    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie();
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishBatchWithConfirms_timeout() throws Exception {
    Builder builder = new Builder();
    final int expectedTimeout = 3000;
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true, expectedTimeout);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
    final TimeoutException expectedCause = new TimeoutException("timeout");
    doThrow(expectedCause).when(channel).waitForConfirmsOrDie(expectedTimeout);
    List<TestEvent> eventList = Collections.singletonList(event);
    final PublishException publishException = assertThrows(PublishException.class, () -> {
      publisher.publishBatch(new RabbitMqBatchEvent<>(eventList, TestEvent.class), publisherConfiguration);
    });
    assertEquals("java.util.concurrent.TimeoutException: timeout", publishException.getCause().getMessage());
    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie(expectedTimeout);
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishBatchWithConfirms_interrupted() throws Exception {
    Builder builder = new Builder();
    final int expectedTimeout = 3000;
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true, expectedTimeout);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
    final InterruptedException expectedCause = new InterruptedException("interrupted");
    List<TestEvent> eventList = Collections.singletonList(event);
    doThrow(expectedCause).when(channel).waitForConfirmsOrDie(expectedTimeout);
    final PublishException publishException = assertThrows(PublishException.class, () -> {
      publisher.publishBatch(new RabbitMqBatchEvent<>(eventList, TestEvent.class), publisherConfiguration);
    });
    assertEquals("java.lang.InterruptedException: interrupted", publishException.getCause().getMessage());
    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie(expectedTimeout);
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishWithConfirms() throws Exception {
    Builder builder = new Builder();
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);

    publisher.publish(event, publisherConfiguration);

    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie();
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishWithConfirmsAndTimeout() throws Exception {
    Builder builder = new Builder();
    final int confirmTimeout = 3000;
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true, confirmTimeout);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);

    publisher.publish(event, publisherConfiguration);

    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie(confirmTimeout);
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishWithConfirms_NackError() throws Exception {
    Builder builder = new Builder();
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
    final IOException expectedCause = new IOException("nack");
    doThrow(expectedCause).when(channel).waitForConfirmsOrDie();
    final PublishException publishException = assertThrows(PublishException.class, () -> {
      publisher.publish(event, publisherConfiguration);
    });
    assertEquals("java.io.IOException: nack", publishException.getCause().getMessage());
    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie();
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishWithConfirms_timeout() throws Exception {
    Builder builder = new Builder();
    final int expectedTimeout = 3000;
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true, expectedTimeout);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
    final TimeoutException expectedCause = new TimeoutException("timeout");
    doThrow(expectedCause).when(channel).waitForConfirmsOrDie(expectedTimeout);
    final PublishException publishException = assertThrows(PublishException.class, () -> {
      publisher.publish(event, publisherConfiguration);
    });
    assertEquals("java.util.concurrent.TimeoutException: timeout", publishException.getCause().getMessage());
    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie(expectedTimeout);
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublishWithConfirms_interrupted() throws Exception {
    Builder builder = new Builder();
    final int expectedTimeout = 3000;
    final PublishConfirmConfiguration withConfirmConfig = new PublishConfirmConfiguration(true, expectedTimeout);
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, withConfirmConfig);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
    final InterruptedException expectedCause = new InterruptedException("interrupted");
    doThrow(expectedCause).when(channel).waitForConfirmsOrDie(expectedTimeout);
    final PublishException publishException = assertThrows(PublishException.class, () -> {
      publisher.publish(event, publisherConfiguration);
    });
    assertEquals("java.lang.InterruptedException: interrupted", publishException.getCause().getMessage());
    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));
    verify(channel).waitForConfirmsOrDie(expectedTimeout);
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublish_with_error() throws Exception {
    Builder builder = new Builder();
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new JsonEncoder<>(), errorHandler, declarations, retryHandler, publishConfirmConfiguration);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    final IOException retryableError = new IOException("someError");
    when(errorHandler.isRetryable(retryableError)).thenReturn(true);
    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
    doThrow(retryableError).when(channel).basicPublish(eq("exchange"),
        eq("routingKey"), propsCaptor.capture(),
        eq("{\"id\":\"theId\",\"booleanValue\":true}".getBytes()));

    Throwable exception = assertThrows(PublishException.class, () -> {
      publisher.publish(event, publisherConfiguration);
    });
    assertEquals("Unable to send message after 3 attempts", exception.getMessage());
    assertEquals("application/json", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testPublish_withEncodeException() throws Exception {
    Builder builder = new Builder();
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, encoder, errorHandler, declarations, retryHandler, publishConfirmConfiguration);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
    doThrow(new EncodeException(new RuntimeException("someError"))).when(encoder).encode(event);

    Throwable exception = assertThrows(PublishException.class, () -> {
      publisher.publish(event, publisherConfiguration);
    });
    assertEquals("Unable to serialize event", exception.getMessage());
  }

  @Test
  public void testPublish_with_custom_MessageConverter() throws Exception {
    Builder builder = new Builder();
    PublisherConfiguration<TestEvent> publisherConfiguration = new PublisherConfiguration(config,
        "exchange", routingKeyFunction, builder, null, new CustomEncoder(), errorHandler, declarations, retryHandler, publishConfirmConfiguration);
    ArgumentCaptor<BasicProperties> propsCaptor = ArgumentCaptor.forClass(BasicProperties.class);

    when(connectionRepository.getConnection(config)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);

    publisher.publish(event, publisherConfiguration);

    verify(channel).basicPublish(eq("exchange"), eq("routingKey"), propsCaptor.capture(),
        eq("Id: theId, BooleanValue: true".getBytes()));
    assertEquals("text/plain", propsCaptor.getValue().getContentType());
  }

  @Test
  public void testClose() {
    publisher.close();
  }

  public static class CustomEncoder implements Encoder<TestEvent> {
    @Override
    public String contentType() {
      return "text/plain";
    }

    @Override
    @SuppressWarnings("boxing")
    public byte[] encode(TestEvent event) throws EncodeException {
      final String str =
          MessageFormat.format("Id: {0}, BooleanValue: {1}", event.getId(), event.isBooleanValue());
      return str.getBytes();
    }
  }
}
