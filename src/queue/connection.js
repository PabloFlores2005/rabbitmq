import amqp from 'amqplib';

export async function connect(rabbitmqConfig, log) {
  const connection = await amqp.connect(rabbitmqConfig);
  connection.on('error', (err) => log.error({ err }, 'connection error'));
  connection.on('close', () => log.info('connection closed'));
  return connection;
}

export async function createChannel(connection, log) {
  const channel = await connection.createChannel();
  channel.on('error', (err) => log.error({ err }, 'channel error'));
  return channel;
}

export async function createConfirmChannel(connection, log) {
  const channel = await connection.createConfirmChannel();
  channel.on('error', (err) => log.error({ err }, 'channel error'));
  return channel;
}

export async function closeQuietly(...resources) {
  for (const resource of resources) {
    if (!resource) continue;
    try {
      await resource.close();
    } catch {
      // already closed
    }
  }
}