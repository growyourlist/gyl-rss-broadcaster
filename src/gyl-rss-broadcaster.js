require('dotenv').config()
const Parser = require('rss-parser')
const dynamodb = require('dynopromise-client')

const sendBroadcast = require('./sendBroadcast')

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

const parser = new Parser()

const getTargetSubscriberTime = () => {
	if (!process.env.TARGET_TIME) {
		return null
	}
	const timeParts = process.env.TARGET_TIME.match(/^(\d\d?):(\d\d?)$/)
	if (!timeParts) {
		return null
	}
	const hour = parseInt(timeParts[1])
	const minute = parseInt(timeParts[2])
	if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
		return null
	}
	return {
		hour: hour,
		minute: minute
	}
}

const broadcastContent = (params, targetDate, subscribersNewThan = null) => {
	const broadcastData = {
		templateId: process.env.TEMPLATE_ID,
		startSendAt: Date.now(),
		tagName: process.env.SUBSCRIBER_TAG,
		params: params,
		targetDate: targetDate,
	}
	const targetTime = getTargetSubscriberTime()
	if (targetTime) {
		broadcastData.targetTime = targetTime
	}
	if (subscribersNewThan) {
		broadcastData.subscribersNewThan = subscribersNewThan
	}
	return sendBroadcast(broadcastData)
}

const getFeedItems = () => parser.parseURL(process.env.FEED_URL)
.then(feed => {
  if (!feed) {
    throw new Error('No feed')
  }
	const feedItems = feed.items
	if (!feedItems || !feedItems.length) {
		throw new Error('RSS feed is empty')
	}

	return feedItems
})

const getLastSent = () => db.get({
	TableName: 'Settings',
	Key: {
		settingName: 'rssSender-LastSent'
	}
})
.then(res => (res.Item && res.Item.value) || null)

const updateLastSent = lastSentItemIso => db.put({
	TableName: 'Settings',
	Item: {
		settingName: 'rssSender-LastSent',
		value: {
			lastSentItemIso: lastSentItemIso,
			lastSentTimestamp: Date.now()
		}
	}
})

const getTargetDate = title => {
	const dateParts = title.match(/(\d\d?) ([a-zA-Z]{3})\S* (\d\d\d\d)/)
	if (!dateParts) {
		throw new Error('Could not work out date from title')
	}
	const months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct',
	'nov','dec']
	const month = months.indexOf(dateParts[2].toLowerCase()) + 1
	if (month < 1) {
		throw new Error('Could not identify month')
	}
	const day = parseInt(dateParts[1])
	if (Number.isNaN(day) || day < 1 || day > 31) {
		throw new Error('Invalid day of the month specified')
	}
	const year = parseInt(dateParts[3])
	const thisYear = (new Date()).getFullYear()
	if (Number.isNaN(year) || year < (thisYear - 1) || year > (thisYear + 1)) {
		throw new Error('Invalid year specified')
	}
	return { day, month, year, }
}

let isRunning = false
const checkAndSend = () => {
	if (isRunning) {
		console.log(`${(new Date).toISOString()}: Exit to avoid overlap`)
		return
	}
	isRunning = true
	Promise.all([
		getFeedItems(),
		getLastSent()
	])
	.then(inputs => {
		const feedItems = inputs[0]
		const lastSent = inputs[1]
		const firstItem = feedItems[0]
		if (!lastSent || !lastSent.lastSentItemIso
			|| new Date(firstItem.isoDate) > new Date(lastSent.lastSentItemIso)) {
			return updateLastSent(firstItem.isoDate)
			.then(() => broadcastContent(
				{
					title: firstItem.title,
					link: firstItem.link,
					content: firstItem.content,
				},
				getTargetDate(firstItem.title)
			))
			.then(() => true)
		}
		else if (firstItem.isoDate === lastSent.lastSentItemIso) {
			return updateLastSent(firstItem.isoDate)
			.then(() => broadcastContent(
				{
					title: firstItem.title,
					link: firstItem.link,
					content:firstItem.content,
				},
				getTargetDate(firstItem.title),
				lastSent.lastSentTimestamp
			))
			.then(() => true)
		}
		return false
	})
	.catch(err => {
		console.log(`${(new Date).toISOString()}: Error triggering broadcast: `
		+ err.message)
	})
	.then(() => isRunning = false)
}

// Check for new RSS content every six hours
checkAndSend()
setInterval(checkAndSend, 21600000)
