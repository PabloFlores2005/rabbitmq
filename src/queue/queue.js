export const QUEUE_OPTIONS = Object.freeze({ durable: true });

export function assertQueue(channel, queueName) {
  return channel.assertQueue(queueName, { ...QUEUE_OPTIONS });
}