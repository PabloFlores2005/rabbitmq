export function publishMessage(channel, queueName, message, { persistent = true } = {}) {
  return new Promise((resolve, reject) => {
    const body = Buffer.from(JSON.stringify(message));
    channel.sendToQueue(
      queueName,
      body,
      {
        persistent,
        contentType: 'application/json',
        messageId: message.messageId,
        timestamp: Math.floor(Date.now() / 1000),
        type: message.eventType,
      },
      (err) => (err ? reject(err) : resolve()),
    );
  });
}