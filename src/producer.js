import { setTimeout as sleep } from 'node:timers/promises';
import pino from 'pino';
import { loadConfig } from './config.js';
import { connect, createConfirmChannel, closeQuietly } from './queue/connection.js';
import { assertQueue } from './queue/queue.js';
import { buildTrafficMessage } from './messages.js';
import { publishPersistent } from './publisher.js';

let config;
try {
  config = loadConfig();
} catch (err) {
  console.error(`Configuration error: ${err.message}`);
  process.exit(1);
}

const log = pino({
  name: 'producer',
  level: config.log.level,
  ...(config.log.pretty && {
    transport: {
      target: 'pino-pretty',
      options: { singleLine: true, translateTime: 'SYS:HH:MM:ss.l', ignore: 'pid,hostname' },
    },
  }),
});

async function main() {
  const { queueName, producer } = config;
  let connection;
  let channel;
  let stopRequested = false;
  let attempted = 0;
  let confirmed = 0;
  let failed = 0;

  const requestStop = (signal) => {
    log.warn({ signal }, 'stop requested, finishing current message');
    stopRequested = true;
  };
  process.once('SIGINT', () => requestStop('SIGINT'));
  process.once('SIGTERM', () => requestStop('SIGTERM'));

  try {
    connection = await connect(config.rabbitmq, log);
    channel = await createConfirmChannel(connection, log);

    const { messageCount, consumerCount } = await assertQueue(channel, queueName);
    log.info({ queue: queueName, messageCount, consumerCount }, 'queue ready');

    for (let i = 1; i <= producer.count && !stopRequested; i += 1) {
      const message = buildTrafficMessage(i);
      attempted += 1;
      try {
        await publishPersistent(channel, queueName, message);
        confirmed += 1;
        log.info(
          {
            sequence: i,
            messageId: message.messageId,
            locationId: message.payload.locationId,
            vehicleCount: message.payload.vehicleCount,
          },
          'message confirmed',
        );
      } catch (err) {
        failed += 1;
        log.error({ err, sequence: i, messageId: message.messageId }, 'message failed');
      }
      if (producer.delayMs > 0) await sleep(producer.delayMs);
    }

    log.info({ requested: producer.count, attempted, confirmed, failed }, 'publish summary');
    if (failed > 0) process.exitCode = 1;
  } catch (err) {
    log.error({ err }, 'fatal error');
    process.exitCode = 1;
  } finally {
    await closeQuietly(channel, connection);
  }
}

await main();