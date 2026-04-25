const mockSubnetworkController = {
  getSubnetworkLimits: jest.fn(),
  createSubnetwork: jest.fn(),
};

jest.mock('../../../controllers/subnetworkController', () => mockSubnetworkController);

const router = require('../../../routes/subnetworks');

describe('Subnetwork route handlers', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('GET /limits delegates to the controller', () => {
    const layer = router.stack.find(
      entry => entry.route && entry.route.path === '/limits' && entry.route.methods.get
    );

    expect(layer).toBeDefined();

    const handler = layer.route.stack[0].handle;
    const req = {};
    const res = {};
    const next = jest.fn();

    handler(req, res, next);

    expect(mockSubnetworkController.getSubnetworkLimits).toHaveBeenCalledWith(req, res, next);
  });

  test('POST / delegates to the controller', () => {
    const layer = router.stack.find(
      entry => entry.route && entry.route.path === '/' && entry.route.methods.post
    );

    expect(layer).toBeDefined();

    const handler = layer.route.stack[0].handle;
    const req = { body: { seeds: ['P1'], name: 'generated' } };
    const res = {};
    const next = jest.fn();

    handler(req, res, next);

    expect(mockSubnetworkController.createSubnetwork).toHaveBeenCalledWith(req, res, next);
  });
});
