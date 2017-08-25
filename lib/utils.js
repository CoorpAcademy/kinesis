function bignumCompare(a, b) {
  if (!a) return -1;
  if (!b) return 1;
  const lengthDiff = a.length - b.length;
  if (lengthDiff !== 0) return lengthDiff;
  return a.localeCompare(b);
}

const nullLogger = {log: function noop() {}};

module.exports = {bignumCompare, nullLogger};
