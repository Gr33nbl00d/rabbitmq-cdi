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
import java.util.List;
import java.util.function.Function;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.Channel;

/**
 * A publisher configuration stores all important settings and options used for publishing and
 * event.
 *
 * @author Patrick Reinhart
 */
final class PublisherConfiguration<T> {
  private final ConnectionConfig config;
  private final BasicProperties basicProperties;
  private final Encoder<T> messageEncoder;
  private final String exchange;
  private final Function<T, String> routingKeyFunction;
  private final ErrorHandler<T> errorHandler;
  private final List<Declaration> declarations;
  private final BasicPropertiesCalculator<T> basicPropertiesCalculator;
  private final PublishRetryHandler<T> publishRetryHandler;
  private final PublishConfirmConfiguration publishConfirmConfiguration;

  PublisherConfiguration(ConnectionConfig config, String exchange,
      Function<T, String> routingKeyFunction,
      Builder basicPropertiesBuilder, BasicPropertiesCalculator<T> basicPropertiesCalculator, Encoder<T> encoder,
      ErrorHandler<T> errorHandler, List<Declaration> declarations, PublishRetryHandler publishRetryHandler, PublishConfirmConfiguration publishConfirmConfiguration) {
    this.config = config;
    this.exchange = exchange;
    this.routingKeyFunction = routingKeyFunction;
    this.messageEncoder = encoder;
    this.errorHandler = errorHandler;
    this.declarations = declarations;
    this.publishRetryHandler = publishRetryHandler;
    this.publishConfirmConfiguration = publishConfirmConfiguration;
    String contentType = messageEncoder.contentType();
    if (contentType != null) {
      basicPropertiesBuilder.contentType(contentType);
    }
    basicProperties = basicPropertiesBuilder.build();
    this.basicPropertiesCalculator = basicPropertiesCalculator;
  }

  /**
   * @return the connection configuration
   */
  ConnectionConfig getConfig() {
    return config;
  }

  List<Declaration> getDeclarations() {
    return declarations;
  }

  @Override
  public String toString() {
    return config.toString();
  }

  void publish(Channel channel, T event) throws EncodeException, IOException {
    byte[] data = messageEncoder.encode(event);
    BasicProperties basicPropertiesToSend = basicProperties;
    if (basicPropertiesCalculator != null) {
      basicPropertiesToSend = this.basicPropertiesCalculator.calculateBasicProperties(this.basicProperties, event);
    }
    channel.basicPublish(exchange, routingKeyFunction.apply(event), basicPropertiesToSend, data);
  }

  public PublishRetryHandler<T> getPublishRetryHandler() {
    return publishRetryHandler;
  }

  public ErrorHandler<T> getErrorHandler() {
    return errorHandler;
  }

  public PublishConfirmConfiguration getPublisherConfirmConfiguration() {
    return this.publishConfirmConfiguration;
  }
}
