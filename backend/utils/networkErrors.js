class NetworkNotFoundError extends Error {
  constructor(filename) {
    super(`Network not found: ${filename}`);
    this.name = 'NetworkNotFoundError';
    this.filename = filename;
  }
}

module.exports = {
  NetworkNotFoundError,
};
