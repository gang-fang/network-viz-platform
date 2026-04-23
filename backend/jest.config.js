// Jest configuration file

module.exports = {
  // The test environment that will be used for testing
  testEnvironment: 'node',
  
  // The directory where Jest should output its coverage files
  coverageDirectory: '<rootDir>/coverage',
  
  // An array of regexp pattern strings that are matched against all test paths
  testMatch: [
    '**/__tests__/**/*.js?(x)',
    '**/?(*.)+(spec|test).js?(x)'
  ],
  
  // An array of regexp pattern strings that are matched against all source file paths
  // before re-running tests in watch mode
  watchPathIgnorePatterns: [
    '/node_modules/',
    '/coverage/'
  ],
  
  // A list of paths to directories that Jest should use to search for files in
  roots: ['<rootDir>'],
  
  // Setup files that will be run before each test
  setupFilesAfterEnv: ['<rootDir>/tests/setup/jest.setup.js'],
  
  // Indicates whether each individual test should be reported during the run
  verbose: true,
  
  // Automatically clear mock calls and instances between every test
  clearMocks: true,
  
  // Test timeout setting
  testTimeout: 60000,
  
  // Collect coverage information from these directories
  collectCoverageFrom: [
    'server.js',
    'routes/**/*.js',
    'controllers/**/*.js',
    'services/**/*.js',
    'models/**/*.js',
    'utils/**/*.js',
    '!**/node_modules/**',
    '!**/tests/**'
  ]
};