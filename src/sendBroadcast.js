const dynamodb = require('dynopromise-client')
const moment = require('moment-timezone')

const debugLog = require('./debugLog')

const dbParams = {
	region: process.env.AWS_REGION,
}
if (process.env.DYNAMODB_ENDPOINT) {
	dbParams.endpoint = process.env.DYNAMODB_ENDPOINT
}
else {
	dbParams.accessKeyId = process.env.AWS_ACCESS_KEY_ID
	dbParams.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY
}

const db = dynamodb(dbParams)

/**
 * Compare function used to sort queue by soonest first.
 */
const comparePutRequests = (a, b) => {
	return b.PutRequest.Item.runAt - a.PutRequest.Item.runAt
}

/**
 * Gets all the subscribers from the database who are confirmed and have not
 * unsubscribed.
 * @return {Promise}
 */
const getAllSubscribers = (opts = {}, subscriberPool = [], LastEvaluatedKey = null) => {

	const scanParams = {
		TableName: 'Subscribers',
		ConsistentRead: true,
		FilterExpression: '#conf <> :false1 and (#unsub = :false2 or '
		+ 'attribute_not_exists(#unsub))',
		ExpressionAttributeNames: {
			'#conf': 'confirmed',
			'#unsub': 'unsubscribed'
		},
		ExpressionAttributeValues: {
			':false1': false,
			':false2': false,
		}
	}

	if (LastEvaluatedKey) {
		scanParams.ExclusiveStartKey = LastEvaluatedKey
	}
	if (opts.tagName) {
		scanParams.FilterExpression += ' and contains(#tags, :tag)'
		scanParams.ExpressionAttributeNames['#tags'] = 'tags'
		scanParams.ExpressionAttributeValues[':tag'] = opts.tagName
	}
	if (opts.subscribersNewThan) {
		scanParams.FilterExpression += ' and #joined > :timestamp'
		scanParams.ExpressionAttributeNames['#joined'] = 'joined'
		scanParams.ExpressionAttributeValues[':timestamp'] = opts.subscribersNewThan
	}

	return db.scan(scanParams)
	.then(results => {
		const subscriberBatch = results.Items
		const newPool = subscriberPool.concat(subscriberBatch)
		if (!results.LastEvaluatedKey) {
			return newPool
		}
		return getAllSubscribers(opts, newPool, results.LastEvaluatedKey)
	})
}

/**
 * Based on the earliest given send data, user timezone and delivery preference,
 * return when the email should be sent.
 * @param  {Number} startSendAt When the email is scheduled to start sending.
 * @param  {String} timezone The timezone of the user.
 * @param  {Object} userDtp The user's delivery time preferences.
 * @return {Number} The time in milliseconds to attempt sending the email.
 */
const getRunAtTime = (startSendAt, timezone, userDtp, targetDate) => {
	if (!timezone) {
		return startSendAt
	}
	const dtp = userDtp || { hour: 9, minute: 30 }
	const sendAtTime = moment(startSendAt).tz(timezone)
	sendAtTime.hour(dtp.hour)
	sendAtTime.minute(dtp.minute)
	sendAtTime.second(Math.floor(Math.random() * 60))
	if (targetDate) {
		sendAtTime.date(targetDate.day)
		sendAtTime.month(targetDate.month - 1)
		sendAtTime.year(targetDate.year)
	}
	return sendAtTime.valueOf()
}

/**
 * Creates a new queue item.
 */
const newQueueItem = (itemData, runAt = Date.now(), targetTime = null,
targetDate = null) => {
	const realRunAt = getRunAtTime(
		runAt,
		itemData.subscriber.timezone,
		targetTime || itemData.subscriber.deliveryTimePreference,
		targetDate,
	)
	const runAtModified = `${realRunAt}${Math.random().toString().substring(1)}`
	return Object.assign({}, itemData, {
		queuePlacement: 'queued',
		runAtModified: runAtModified,
		runAt: realRunAt,
		attempts: 0,
		failed: false,
		completed: false,
	})
}

const lockQueue = () => {
	return db.put({
		TableName: 'Settings',
		Item: {
			'settingName': 'isDoingBroadcast',
			'value': true,
		}
	})
}

const unlockQueue = () => {
	return db.put({
		TableName: 'Settings',
		Item: {
			'settingName': 'isDoingBroadcast',
			'value': false,
		}
	})
}

/**
 * Validate that the queue is ready for a new broadcast.
 */
const validateConditions = () => {
	return db.get({
		TableName: 'Settings',
		ConsistentRead: true,
		Key: { 'settingName': 'isDoingBroadcast' }
	})
	.then(res => {
		if (res && res.Item && res.Item.value === true) {
			throw new Error('Queue is currently locked')
		}
		return true
	})
}


const processBatches = (batches, emailCounter = 0) => new Promise(resolve => {
	if (!batches.length) {
		return resolve(emailCounter)
	}
	debugLog(`${(new Date).toISOString()}: Batch count: ${batches.length}`)
	const nextBatch = batches.pop()
	return db.batchWrite({ RequestItems: { Queue: nextBatch } })
	.then(result => {
		const unprocessedItems = result.UnprocessedItems
		&& result.UnprocessedItems.Queue
		if (unprocessedItems && Array.isArray(unprocessedItems)) {
			emailCounter += (nextBatch.length - unprocessedItems.length)
			debugLog(`${(new Date).toISOString()}: Requeuing `
			+ `${unprocessedItems.length} items`)
			batches.push(unprocessedItems)
		}
		else {
			emailCounter += nextBatch.length
		}
		return setTimeout(
			() => resolve(processBatches(batches, emailCounter)),
			Math.random() * 800
		)
	})
	.catch(err => {
		if (err.retryable) {
			debugLog(`${(new Date).toISOString()}: Requeuing error items`)
			batches.push(nextBatch)
		}
		else {
			console.log(`${(new Date).toISOString()}: Failed batch: ${err.message}`)
		}
		return setTimeout(
			() => resolve(processBatches(batches, emailCounter)),
			Math.random() * 500
		)
	})
})

/**
 * Sends a broadcast email to all applicable subscribers.
 * @param  {Object} broadcastData Defines the broadcast to send.
 * @return {Promise}
 */
const sendBroadcast = broadcastData => validateConditions()
.then(() => lockQueue())
.then(() => getAllSubscribers({
	tagName: broadcastData.tagName,
	subscribersNewThan: broadcastData.subscribersNewThan,
}))
.then(subscribers => {
	if (!subscribers || !subscribers.length) {
		debugLog(`${(new Date).toISOString()}: No subscribers found for broadcast`)
		return 0
	}
	console.log(`${(new Date).toISOString()}: Preparing broadcast for `
	+ `${subscribers.length} subscribers`)

	const putRequests = subscribers.map(subscriber => ({
		PutRequest: {
			Item: newQueueItem(
				{
					type: 'send email',
					subscriber: subscriber,
					subscriberId: subscriber.subscriberId,
					templateId: broadcastData.templateId,
					params: broadcastData.params,
					tagReason: broadcastData.tagName,
				},
				broadcastData.startSendAt,
				broadcastData.targetTime,
				broadcastData.targetDate,
			)
		}
	}))

	// Sort the queue items so that those that should be acted upon first are
	// added to the queue first.
	putRequests.sort(comparePutRequests)

	// Batch the put requests for efficient writing to the db.
	let currentBatch = []
	const batches = []
	const batchThreshold = 25
	putRequests.forEach(putRequest => {
		if (currentBatch.length === batchThreshold) {
			batches.push(currentBatch)
			currentBatch = []
		}
		currentBatch.push(putRequest)
	})
	batches.push(currentBatch)
	return processBatches(batches)
})
.then(emailCount => {
	if (emailCount) {
		console.log(`${(new Date).toISOString()}: Added `
		+ `${emailCount} email/s to the queue`)
	}
})
.catch(err => console.log(`${(new Date).toISOString()}: Error while sending `
+ `broadcast: ${err.message}`))
.then(() => unlockQueue())

module.exports = sendBroadcast
