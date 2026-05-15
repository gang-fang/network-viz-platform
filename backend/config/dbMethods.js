const db = require('./database');

function callDbMethod(methodName, args) {
  return new Promise((resolve, reject) => {
    const method = db[methodName];
    if (typeof method !== 'function') {
      reject(new Error(`Database method ${methodName} is unavailable`));
      return;
    }

    method.call(db, ...args, (err, result) => {
      if (err) reject(err);
      else resolve(result);
    });
  });
}

module.exports = {
  db,
  dbAll: (...args) => callDbMethod('all', args),
  dbGet: (...args) => callDbMethod('get', args),
  dbRun: (...args) => callDbMethod('run', args),
};
