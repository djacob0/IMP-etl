function getManilaTimestamp() {
    return new Date().toLocaleString('en-PH', {
      timeZone: 'Asia/Manila',
      hour12: false
    });
  }

  module.exports = { getManilaTimestamp };
