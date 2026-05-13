module.exports = {
  testEnvironment: 'node',

  coverageDirectory: '<rootDir>/coverage',

  testMatch: [
    '**/__tests__/**/*.js?(x)',
    '**/?(*.)+(spec|test).js?(x)'
  ],

  watchPathIgnorePatterns: [
    '/node_modules/',
    '/coverage/'
  ],

  roots: ['<rootDir>'],

  setupFilesAfterEnv: ['<rootDir>/tests/setup/jest.setup.js'],

  verbose: true,

  clearMocks: true,

  testTimeout: 60000,

  collectCoverageFrom: [
    'server.js',
    'routes/**/*.js',
    'controllers/**/*.js',
    'services/**/*.js',
    'utils/**/*.js',
    '!**/node_modules/**',
    '!**/tests/**'
  ]
};
