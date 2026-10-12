import { setTimeout as sleep } from 'node:timers/promises';

export function createProcessor({ processingMs, failRate }) {
  return async function processTrafficMessage(_message) {
    // Simulated work: makes in-flight (unacknowledged) messages observable in the UI and logs.
    await sleep(processingMs);

    if (failRate > 0 && Math.random() < failRate) {
      throw new Error('simulated transient failure');
    }
  };
}