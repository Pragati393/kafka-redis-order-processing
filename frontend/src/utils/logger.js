const LOG_LEVELS = {
  DEBUG: 'debug',
  INFO: 'info',
  WARN: 'warn',
  ERROR: 'error'
};

const getTimestamp = () => new Date().toISOString();

const formatMessage = (level, message, data) => {
  const timestamp = getTimestamp();
  const dataString = data ? JSON.stringify(data, null, 2) : '';
  return `[${timestamp}] [${level.toUpperCase()}] ${message} ${dataString}`;
};

const logger = {
  debug: (message, data) => {
    if (process.env.NODE_ENV === 'development') {
      console.debug(formatMessage(LOG_LEVELS.DEBUG, message, data));
    }
  },

  info: (message, data) => {
    console.info(formatMessage(LOG_LEVELS.INFO, message, data));
  },

  warn: (message, data) => {
    console.warn(formatMessage(LOG_LEVELS.WARN, message, data));
  },

  error: (message, error) => {
    console.error(
      formatMessage(LOG_LEVELS.ERROR, message, {
        message: error?.message,
        stack: error?.stack,
        ...error
      })
    );
  }
};

export default logger; 