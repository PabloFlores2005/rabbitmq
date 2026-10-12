import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';

const ENV_FILE = fileURLToPath(new URL('../.env', import.meta.url));
 
const LOG_FORMATS = ['json', 'pretty'];
 
function readInt(name, fallback, min = 0) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value < min) {
    throw new Error(`${name} must be an integer >= ${min}, got "${raw}"`);
  }
  return value;
}
 
function readBool(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  if (raw === 'true') return true;
  if (raw === 'false') return false;
  throw new Error(`${name} must be "true" or "false", got "${raw}"`);
}
 
function readFloat(name, fallback, min, max) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new Error(`${name} must be a number between ${min} and ${max}, got "${raw}"`);
  }
  return value;
}
 
export function loadConfig() {
  dotenv.config({ path: ENV_FILE, quiet: true });
 
  const missing = ['RABBITMQ_USER', 'RABBITMQ_PASS'].filter((name) => !process.env[name]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
 
  const logFormat = process.env.LOG_FORMAT ?? 'json';
  if (!LOG_FORMATS.includes(logFormat)) {
    throw new Error(`LOG_FORMAT must be one of ${LOG_FORMATS.join(', ')}, got "${logFormat}"`);
  }
 
  return Object.freeze({
    rabbitmq: Object.freeze({
      protocol: 'amqp',
      hostname: process.env.RABBITMQ_HOST ?? 'localhost',
      port: readInt('RABBITMQ_PORT', 5672, 1),
      username: process.env.RABBITMQ_USER,
      password: process.env.RABBITMQ_PASS,
      vhost: process.env.RABBITMQ_VHOST ?? '/',
    }),
    queueName: process.env.QUEUE_NAME ?? 'demo-queue',
    log: Object.freeze({
      level: process.env.LOG_LEVEL ?? 'info',
      pretty: logFormat === 'pretty',
    }),
    producer: Object.freeze({
      count: readInt('COUNT', 10, 1),
      delayMs: readInt('DELAY_MS', 0),
      // false publishes transient messages
      persistent: readBool('PERSISTENT', true),
    }),
    consumer: Object.freeze({
      id: process.env.CONSUMER_ID ?? `consumer-${process.pid}`,
      prefetch: readInt('PREFETCH', 1, 1),
      processingMs: readInt('PROCESSING_MS', 1000), // simulated work per message
      failRate: readFloat('FAIL_RATE', 0, 0, 1), // simulated transient failures (0 = never)
    }),
  });
}