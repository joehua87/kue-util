#!/usr/bin/env node

// @flow

import kue from 'kue'
import { delay } from 'bluebird'
import inquirer from 'inquirer'
import clearJobsByStatuses from '../clearJobsByStatuses'

const queueStatuses = [
  'inactive',
  'active',
  'failed',
  'complete',
]

let queue

inquirer.prompt([
  {
    type: 'input',
    name: 'redisHost',
    message: 'Your Redis Host',
    default: 'localhost',
  },
  {
    type: 'checkbox',
    message: 'Select jobs which want to be clear',
    name: 'statuses',
    choices: queueStatuses.map(name => ({ name })),
    validate(answer) {
      if (answer.length < 1) {
        return 'You must choose at least one topping.'
      }
      return true
    },
  },
]).then((answers) => {
  const { redisHost, statuses } = answers
  queue = kue.createQueue({
    redis: { host: redisHost },
  })
  const promises = clearJobsByStatuses(redisHost, statuses)
  // Wait 2 seconds before first promise is pushed
  return delay(2000).then(() => promises)
})
  .then(() => {
    queue.shutdown(5000, (err) => {
      console.log('Kue shutdown: ', err || '')
      process.exit(0)
    })
  })
