// Jest setup file

// Set test environment variables
process.env.NODE_ENV = 'test';
process.env.PORT = 3001; // Use a different port for testing

beforeAll(async () => {
  console.log('Starting test suite');
});

afterAll(async () => {
  console.log('Test suite completed');
});