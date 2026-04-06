/**
 * Redirect all console output to stderr BEFORE any other imports.
 * This prevents module-level console.log() calls (e.g. SessionManager init)
 * from polluting stdout, which is reserved for JSON IPC with Python.
 */
const write = (args) => {
  try {
    process.stderr.write(`${args.map((item) => {
      if (typeof item === 'string') return item;
      try {
        return JSON.stringify(item);
      } catch {
        return String(item);
      }
    }).join(' ')}\n`);
  } catch {
    // noop
  }
};

console.log = (...args) => write(args);
console.info = (...args) => write(args);
console.warn = (...args) => write(args);
console.error = (...args) => write(args);
console.debug = (...args) => write(args);
console.trace = (...args) => write(args);
console.dir = (...args) => write(args);
console.table = (...args) => write(args);
