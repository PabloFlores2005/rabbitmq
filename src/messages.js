import { randomUUID } from 'node:crypto';

export const EVENT_TYPE = 'traffic.observation.recorded';

const LOCATIONS = ['LOC-001', 'LOC-002', 'LOC-003', 'LOC-004'];

const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

export function buildTrafficMessage(sequence) {
  return {
    messageId: randomUUID(),
    timestamp: new Date().toISOString(),
    eventType: EVENT_TYPE,
    payload: {
      sequence, // position in the batch: helps analyze distribution and ordering
      locationId: LOCATIONS[sequence % LOCATIONS.length],
      vehicleCount: randomInt(1, 50),
      avgSpeedKmh: randomInt(20, 100),
    },
  };
}

export class InvalidMessageError extends Error {
  constructor(reason, options) {
    super(reason, options);
    this.name = 'InvalidMessageError';
  }
}

export function parseTrafficMessage(body) {
  let data;
  try {
    data = JSON.parse(body.toString('utf8'));
  } catch (cause) {
    throw new InvalidMessageError('body is not valid JSON', { cause });
  }

  const payload = data?.payload;
  const valid =
    typeof data?.messageId === 'string' &&
    typeof data?.timestamp === 'string' &&
    data?.eventType === EVENT_TYPE &&
    payload !== null &&
    typeof payload === 'object' &&
    Number.isInteger(payload.sequence) &&
    typeof payload.locationId === 'string' &&
    Number.isInteger(payload.vehicleCount) &&
    Number.isInteger(payload.avgSpeedKmh);

  if (!valid) {
    throw new InvalidMessageError('message does not match the traffic observation schema');
  }
  return data;
}