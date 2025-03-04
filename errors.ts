type Success<T> = {
	data: T
	error: null
}

type Failure<E> = {
	data: null
	error: E
}

type Result<T, E = Error> = Success<T> | Failure<E>

export async function tryCatch<T, E extends Error = Error>(
	promise: Promise<T>
): Promise<Result<T, E>> {
	try {
		const data = await promise
		return { data, error: null }
	} catch (error) {
		const finalError = error as E
		return { data: null, error: finalError }
	}
}

export function trySync<T, E extends Error = Error>(fn: () => T): Result<T, E> {
	try {
		const data = fn()
		return { data, error: null }
	} catch (error) {
		const finalError = error as E
		return { data: null, error: finalError }
	}
}

export class WrappedError extends Error {
	cause: Error | null

	constructor(message: string, cause?: Error) {
		super(message)
		this.name = "WrappedError"
		this.cause = cause || null

		if (Error.captureStackTrace) {
			Error.captureStackTrace(this, WrappedError)
		}
	}

	unwrap(): Error | null {
		return this.cause
	}

	toString(): string {
		if (this.cause) {
			return `${this.message}: ${this.cause.message}`
		}
		return this.message
	}
}

export function wrap<E extends Error>(cause: E, message: string): WrappedError {
	return new WrappedError(message, cause)
}

export const Errors = Object.freeze({
	try: tryCatch,
	trySync,
	wrap,
	WrappedError
})
