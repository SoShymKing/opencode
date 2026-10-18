// Workerd cannot report free memory; preserve serial admission, without promising a memory cap or OOM protection.
export const availableParallelism = () => 1
export const freemem = () => Number.MAX_SAFE_INTEGER
