#!/usr/bin/env node

// @flow

import kue from 'kue'
import inquirer from 'inquirer'

const queueStatuses = [
  'active',
  'failed',
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
    prefix: 'q',
    redis: {
      host: redisHost,
    },
  })

  statuses.forEach((status) => {
    queue[status]((err, ids) => {
      ids.forEach((id) => {
        kue.Job.get(id, (_err, job) => {
          job.inactive((___err, _job) => {
            console.log(`inactive ${_job.id}`)
          })
        })
      })
    })
  })
})
