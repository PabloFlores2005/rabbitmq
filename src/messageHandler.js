import { parseTrafficMessage } from './messages.js';

export function createMessageHandler({ channel, log, processMessage }) {
  const pending = new Set();

  function settle(action) {
    try {
      action();
      return true;
    } catch (err) {
      log.error({ err }, 'could not settle message; the broker will redeliver it');
      return false;
    }
  }

  async function handle(msg) {
    const { deliveryTag, redelivered } = msg.fields;

    let message;
    try {
      message = parseTrafficMessage(msg.content);
    } catch (err) {
      log.error(
        { err, deliveryTag, redelivered, rawBody: msg.content.toString('utf8').slice(0, 200) },
        'invalid message rejected (not requeued)',
      );
      settle(() => channel.nack(msg, false, false));
      return;
    }

    const context = {
      deliveryTag,
      redelivered,
      messageId: message.messageId,
      sequence: message.payload.sequence,
    };

    log.info(context, 'message received');

    try {
      await processMessage(message);
    } catch (err) {
      if (redelivered) {
        log.error({ err, ...context }, 'processing failed again, message discarded');
        settle(() => channel.nack(msg, false, false));
      } else {
        log.warn({ err, ...context }, 'processing failed, message requeued for one retry');
        settle(() => channel.nack(msg, false, true));
      }
      return;
    }

    if (settle(() => channel.ack(msg))) {
      log.info(
        {
          ...context,
          locationId: message.payload.locationId,
          vehicleCount: message.payload.vehicleCount,
        },
        'message processed and acknowledged',
      );
    }
  }

  return {
    handle(msg) {
      const task = handle(msg).finally(() => pending.delete(task));
      pending.add(task);
      return task;
    },
    idle: () => Promise.allSettled([...pending]),
  };
}