package com.bandwidth.sqs.consumer;

import com.amazonaws.services.sqs.model.Message;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Generated;

/**
 * Immutable implementation of {@link TimedMessage}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTimedMessage.builder()}.
 */
@SuppressWarnings({"all"})
@Generated({"Immutables.generator", "TimedMessage"})
public final class ImmutableTimedMessage extends TimedMessage {
  private final Message message;
  private final Instant receivedTime;

  private ImmutableTimedMessage(ImmutableTimedMessage.Builder builder) {
    this.message = builder.message;
    this.receivedTime = builder.receivedTime != null
        ? builder.receivedTime
        : Objects.requireNonNull(super.getReceivedTime(), "receivedTime");
  }

  private ImmutableTimedMessage(Message message, Instant receivedTime) {
    this.message = message;
    this.receivedTime = receivedTime;
  }

  /**
   * @return The value of the {@code message} attribute
   */
  @Override
  public Message getMessage() {
    return message;
  }

  /**
   * @return The value of the {@code receivedTime} attribute
   */
  @Override
  public Instant getReceivedTime() {
    return receivedTime;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TimedMessage#getMessage() message} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for message
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimedMessage withMessage(Message value) {
    if (this.message == value) return this;
    Message newValue = Objects.requireNonNull(value, "message");
    return new ImmutableTimedMessage(newValue, this.receivedTime);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TimedMessage#getReceivedTime() receivedTime} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for receivedTime
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimedMessage withReceivedTime(Instant value) {
    if (this.receivedTime == value) return this;
    Instant newValue = Objects.requireNonNull(value, "receivedTime");
    return new ImmutableTimedMessage(this.message, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTimedMessage} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTimedMessage
        && equalTo((ImmutableTimedMessage) another);
  }

  private boolean equalTo(ImmutableTimedMessage another) {
    return message.equals(another.message)
        && receivedTime.equals(another.receivedTime);
  }

  /**
   * Computes a hash code from attributes: {@code message}, {@code receivedTime}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 31;
    h = h * 17 + message.hashCode();
    h = h * 17 + receivedTime.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TimedMessage} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TimedMessage{"
        + "message=" + message
        + ", receivedTime=" + receivedTime
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link TimedMessage} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TimedMessage instance
   */
  public static ImmutableTimedMessage copyOf(TimedMessage instance) {
    if (instance instanceof ImmutableTimedMessage) {
      return (ImmutableTimedMessage) instance;
    }
    return ImmutableTimedMessage.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTimedMessage ImmutableTimedMessage}.
   * @return A new ImmutableTimedMessage builder
   */
  public static ImmutableTimedMessage.Builder builder() {
    return new ImmutableTimedMessage.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTimedMessage ImmutableTimedMessage}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  public static final class Builder {
    private static final long INIT_BIT_MESSAGE = 0x1L;
    private long initBits = 0x1L;

    private Message message;
    private Instant receivedTime;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TimedMessage} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TimedMessage instance) {
      Objects.requireNonNull(instance, "instance");
      withMessage(instance.getMessage());
      withReceivedTime(instance.getReceivedTime());
      return this;
    }

    /**
     * Initializes the value for the {@link TimedMessage#getMessage() message} attribute.
     * @param message The value for message 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder withMessage(Message message) {
      this.message = Objects.requireNonNull(message, "message");
      initBits &= ~INIT_BIT_MESSAGE;
      return this;
    }

    /**
     * Initializes the value for the {@link TimedMessage#getReceivedTime() receivedTime} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TimedMessage#getReceivedTime() receivedTime}.</em>
     * @param receivedTime The value for receivedTime 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder withReceivedTime(Instant receivedTime) {
      this.receivedTime = Objects.requireNonNull(receivedTime, "receivedTime");
      return this;
    }

    /**
     * Builds a new {@link ImmutableTimedMessage ImmutableTimedMessage}.
     * @return An immutable instance of TimedMessage
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTimedMessage build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableTimedMessage(this);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<String>();
      if ((initBits & INIT_BIT_MESSAGE) != 0) attributes.add("message");
      return "Cannot build TimedMessage, some of required attributes are not set " + attributes;
    }
  }
}
