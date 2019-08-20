const isDebug = process.env.DEBUG === 'true'
const debugLog = message => {
	if (isDebug) {
		console.log(message)
	}
}

module.exports = debugLog
