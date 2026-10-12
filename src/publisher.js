export function publishPersistent(channel, queueName, message) {
  return new Promise((resolve, reject) => {
    const body = Buffer.from(JSON.stringify(message));
    channel.sendToQueue(
      queueName,
      body,
      {
        persistent: true, // delivery mode 2: ask the broker to write the message to disk
        contentType: 'application/json',
        messageId: message.messageId,
        timestamp: Math.floor(Date.now() / 1000),
        type: message.eventType,
      },
      (err) => (err ? reject(err) : resolve()),
    );
  });
}