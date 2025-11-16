public class DriverManager {

    private static void processMessage(String messageStr) {
        String errorMessage = "";
        Map<String, String> carrier = null;
        String[] msgs = messageStr.split(":");
        String invocationHandlerKey = getTaskKey(msgs[0], msgs[1]);

        // TODO this will be removed in the future
        // check is there traceparent in the message, ServiceGroupID:TaskID:status:traceparent-123&key=value
        if (messageStr.contains("traceparent")) {
            String carrierString = msgs[3];
            carrier = extractCarrierFromString(carrierString);
        }

        if (msgs.length > 3) {
            String[] errorMessages = Arrays.copyOfRange(msgs, 2, msgs.length);
            errorMessage = String.join(":", errorMessages);
        }

        try {
            Context context = createContext(carrier);
            try (Scope scope = context.makeCurrent()) {
                Span span = getTracer().spanBuilder("DriverManager.onMessage")
                        .setAttribute("serviceGroupId", msgs[0])
                        .setAttribute("taskId", msgs[1])
                        .startSpan();
                try (Scope ignored = span.makeCurrent()) {
                    ServiceInvocationHandler handler = serviceTaskMap.get(invocationHandlerKey);
                    if (handler == null) {
                        log.error("Failed to get handler for invocation handler: {}",
                                invocationHandlerKey);
                        return;
                    }
                    executorService.submit(
                            new ActionResponseRunner(msgs[0], msgs[1], msgs[2], handler,
                                    errorMessage, context));
                } catch (Exception e) {
                    span.recordException(e);
                    throw e;
                } finally {
                    span.end();
                }
            }
        } finally {
            serviceTaskMap.remove(invocationHandlerKey);
        }
    }

    private static void startSubscription() {
        executorService.submit(() -> {
            // Subscribe to the Redis channel
            //noinspection InfiniteLoopStatement
            while (true) {
                try {
                    log.warn("connecting and subscribe");
                    // Create stream group if not exists
                    RStream<byte[], byte[]> stream = redissonClient.getStream(REDIS_TASK_STREAM, ByteArrayCodec.INSTANCE);
                    stream.createGroup("gofish", StreamMessageId.NEWEST);
                } catch (Exception e) {
                    log.warn("Failed to create consumer group, it may already exist: {}", e.getMessage());
                }

                // Subscribe to Redis Stream using consumer group
                RStream<String, String> engineStream = redissonClient.getStream(REDIS_ENGINE_CHANNEL, StringCodec.INSTANCE);
                String consumerName = "consumer-" + Thread.currentThread().getId();

                // Keep reading messages from the stream
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        // Read messages from stream with consumer group (current Redisson API)
                        StreamReadGroupArgs args = StreamReadGroupArgs.greaterThan(StreamMessageId.NEVER_DELIVERED)
                                .count(5)
                                .timeout(Duration.ofSeconds(5));

                        Map<StreamMessageId, Map<String, String>> messages = engineStream.readGroup(
                                "gofish",
                                consumerName,
                                args
                        );

                        if (messages != null && !messages.isEmpty()) {
                            // Single stream API returns Map<StreamMessageId, Map<String, String>>
                            for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
                                StreamMessageId messageId = entry.getKey();
                                Map<String, String> messageData = entry.getValue();

                                // Extract message content from stream
                                String messageContent = messageData.get("message");
                                if (messageContent != null) {
                                    // Process message directly without MessageListener
                                    processMessage(messageContent);

                                    // Acknowledge the message
                                    engineStream.ack("gofish", messageId);
                                }
                            }
                        }

                        // Check if client is still connected
                        if (redissonClient.isShutdown()) {
                            throw new RuntimeException("Redisson client is shutdown");
                        }

                    } catch (Exception e) {
                        log.error("Error reading from stream", e);
                        Thread.sleep(1000); // Wait before retry
                    }
                }
            }
        });
    }
}
