// @flow

import R from 'ramda'
import { map as mapP } from 'bluebird'
import kue from 'kue'
import Redis from 'ioredis'

const debug = require('debug')('kue-util:enqueue')

// Only use 1 connection of redis to improve perfomance, I guess

export async function getExistsKey(redis: any, { queueName, items, uniqueKey }: {
  queueName: string,
  items: Array<string>,
  uniqueKey: string,
}): Promise<Array<string>> {
  const setName = `set:${queueName}`
  const keys = R.pluck(uniqueKey, items)

  const pipe = redis.pipeline()
  keys.forEach(item => pipe.sismember(setName, item))
  const result = await pipe.exec()
  // debug('exec redis return', result)

  const mapIndexed = R.addIndex(R.map)
  const existsKeys = R.pipe(
    mapIndexed((item, idx) => ({
      key: keys[idx],
      value: item[1],
    })),
    R.filter(x => x.value === 1), // value = 1 mean exists in redis set
    R.pluck('key'),
  )(result)
  // debug('exists keys', existsKeys.join(', '))
  debug(`Got ${existsKeys.length} exists keys from set ${setName}`)
  return existsKeys
}

export default async function enqueue({
  redisHost,
  queueName,
  items,
  uniqueKey,
  force = false,
  delay = 0,
  attempts = 3,
  concurrency = 16,
}: {
  redisHost: string,
  queueName: string,
  items: Array<any>,
  uniqueKey: string, // Use this to prevent run duplicated job
  force?: boolean,
  delay?: number,
  attempts?: number,
  concurrency?: number,
}) {
  debug(`Start enqueue ${items.length} to ${queueName}`)
  const redis = new Redis({
    host: redisHost,
  })
  const queue = kue.createQueue({
    redis: { host: redisHost },
  })
  let i

  // Check if item is already in queue
  // Because kue is use job id as an key, so we need another redis set to keep queueName:uniqueKey
  const existsKeys = await getExistsKey(redis, { queueName, items, uniqueKey })
  // Only enqueue items that not exists
  const itemsToEnqueue = force ? items : items.filter(item => !existsKeys.includes(item[uniqueKey]))

  const promises = mapP(
    itemsToEnqueue,
    itemData => (
      new Promise((resolve, reject) => {
        queue.create(queueName, itemData)
        .attempts(attempts)
        .delay(delay)
        .save((err) => {
          i += 1
          if (i % 1000 === 0) debug(`Enqueue ${i} items`)
          if (err) {
            reject(err)
            return
          }
          resolve()
        })
      })
    ),
    { concurrency },
  )
  await Promise.all(promises)

  const setName = `set:${queueName}` // set to prevent duplicated

  // Marked as enqueue successfully
  if (itemsToEnqueue.length > 0) {
    const enqueuedKeys = R.pluck(uniqueKey, itemsToEnqueue)
    debug(`Add to set ${setName} ${enqueuedKeys.length} items to marked as enqueued`)
    // debug('Add to set to marked as enqueued', enqueuedKeys)
    await redis.sadd(setName, enqueuedKeys)
  }

  debug(`Enqueue ${itemsToEnqueue.length} items`)

  queue.shutdown(5000, (err) => {
    debug('Kue shutdown: ', err || '')
    process.exit(0)
  })
}
