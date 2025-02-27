import { RateLimitError } from "openai"
import { tryCatch } from "./try-catch"

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms))
}

type ErrorConstructor = typeof RateLimitError | typeof Error

export async function retryWithExponentialBackoff<T>(
	fn: () => Promise<T>,
	initialDelay = 1000,
	exponentialBase = 2,
	jitter = true,
	maxRetries = 10,
	errors: ErrorConstructor[] = [RateLimitError]
): Promise<T> {
	let delay = initialDelay
	let retries = 0
	while (true) {
		const result = await tryCatch<T>(fn())

		if (result.error === null) {
			return result.data
		}

		if (!errors.some((E) => result.error instanceof E)) {
			throw result.error
		}

		retries++
		if (retries > maxRetries) {
			throw new Error(`Maximum number of retries (${maxRetries}) exceeded.`)
		}

		const sleepTime = delay * (jitter ? 1 + Math.random() : 1)
		await sleep(sleepTime)
		delay *= exponentialBase
	}
}
