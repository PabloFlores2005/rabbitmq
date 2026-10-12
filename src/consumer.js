import pino from 'pino';
import { loadConfig } from './config.js';
import { connect, createChannel, closeQuietly } from './queue/connection.js';
import { assertQueue } from './queue/queue.js';
import { createProcessor } from './processor.js';
import { createMessageHandler } from './messageHandler.js';

let config;
try {
  config = loadConfig();
} catch (err) {
  console.error(`Configuration error: ${err.message}`);
  process.exit(1);
}

const log = pino({
  name: config.consumer.id,
  level: config.log.level,
  ...(config.log.pretty && {
    transport: {
      target: 'pino-pretty',
      options: { singleLine: true, translateTime: 'SYS:HH:MM:ss.l', ignore: 'pid,hostname' },
    },
  }),
});

async function main() {
  const { queueName, consumer } = config;
  let connection;
  let channel;
  let shuttingDown = false;

  let requestStop;
  const stopped = new Promise((resolve) => {
    requestStop = resolve;
  });

  const onSignal = (signal) => {
    log.warn({ signal }, 'shutdown requested; finishing in-flight messages (Ctrl+C again to force)');
    shuttingDown = true;
    requestStop();
  };
  process.once('SIGINT', () => onSignal('SIGINT'));
  process.once('SIGTERM', () => onSignal('SIGTERM'));

  try {
    connection = await connect(config.rabbitmq, log);
    connection.once('close', () => {
      if (!shuttingDown) {
        log.error('connection lost');
        process.exitCode = 1;
        requestStop();
      }
    });

    channel = await createChannel(connection, log);
    await assertQueue(channel, queueName);

    await channel.prefetch(consumer.prefetch);

    const handler = createMessageHandler({
      channel,
      log,
      processMessage: createProcessor(consumer),
    });

    const { consumerTag } = await channel.consume(
      queueName,
      (msg) => {
        if (msg === null) {
          log.error('consumer cancelled by the broker (queue deleted?)');
          process.exitCode = 1;
          requestStop();
          return;
        }
        handler.handle(msg);
      },
      { noAck: false }, // manual acknowledgments
    );

    log.info(
      { queue: queueName, prefetch: consumer.prefetch, processingMs: consumer.processingMs },
      'consumer started, waiting for messages',
    );

    await stopped;

    await channel.cancel(consumerTag);
    await handler.idle();
    log.info('in-flight messages settled');
  } catch (err) {
    log.error({ err }, 'fatal error');
    process.exitCode = 1;
  } finally {
    await closeQuietly(channel, connection);
  }
}

await main();