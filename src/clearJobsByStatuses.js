// @flow

import kue from 'kue'

export default function clearJobsByStatus(
  redisHost: string,
  statuses: Array<string>,
): Promise<Array<string>> {
  const promises = []
  const queue = kue.createQueue({
    redis: { host: redisHost },
  })
  statuses.forEach((item) => {
    queue[item]((err, ids) => { // others are activeCount, completeCount, failedCount, delayedCount
      ids.forEach((id) => {
        kue.Job.get(id, (_err, job) => {
          if (_err) return
          // Push promises here to wait
          promises.push(new Promise((resolve, reject) => {
            job.remove((__err) => {
              if (__err) reject(__err)
              resolve(job.id)
            })
          }))
        })
      })
    })
  })
  return promises
}
