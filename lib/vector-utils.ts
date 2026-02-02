import { InvalidVectorError, VectorDimensionError } from './errors'

/**
 * Compute the dot product of two vectors.
 * Returns a scalar value.
 */
export function dotProduct(a: number[], b: number[]): number {
  let sum = 0
  for (let i = 0; i < a.length; i++) {
    sum += a[i] * b[i]
  }
  return sum
}

/**
 * Compute the magnitude (L2 norm) of a vector.
 */
export function magnitude(v: number[]): number {
  let sum = 0
  for (let i = 0; i < v.length; i++) {
    sum += v[i] * v[i]
  }
  return Math.sqrt(sum)
}

/**
 * Compute cosine similarity between two vectors.
 * Returns a value between -1 and 1, where 1 means identical direction.
 */
export function cosineSimilarity(a: number[], b: number[]): number {
  const dot = dotProduct(a, b)
  const magA = magnitude(a)
  const magB = magnitude(b)

  if (magA === 0 || magB === 0) {
    return 0
  }

  return dot / (magA * magB)
}

/**
 * Compute Euclidean distance between two vectors.
 * Returns a value >= 0, where 0 means identical vectors.
 */
export function euclideanDistance(a: number[], b: number[]): number {
  let sum = 0
  for (let i = 0; i < a.length; i++) {
    const diff = a[i] - b[i]
    sum += diff * diff
  }
  return Math.sqrt(sum)
}

/**
 * Normalize a vector to unit length.
 * Returns a new vector with magnitude 1.
 */
export function normalizeVector(v: number[]): number[] {
  const mag = magnitude(v)
  if (mag === 0) {
    return v.slice()
  }
  return v.map(x => x / mag)
}

/**
 * Validate that a value is a valid vector of the expected dimension.
 * Throws InvalidVectorError or VectorDimensionError on failure.
 */
export function validateVector(v: unknown, dimension: number): asserts v is number[] {
  if (!Array.isArray(v)) {
    throw new InvalidVectorError('Vector must be an array')
  }

  if (v.length !== dimension) {
    throw new VectorDimensionError(dimension, v.length)
  }

  for (let i = 0; i < v.length; i++) {
    if (typeof v[i] !== 'number' || !Number.isFinite(v[i])) {
      throw new InvalidVectorError(`Vector element at index ${i} must be a finite number`)
    }
  }
}
